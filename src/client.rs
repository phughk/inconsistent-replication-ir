use crate::io::{IRClientStorage, IRNetwork};
use crate::server::View;
use crate::types::{DecideFunction, IRMessage, NodeID};
use crate::utils::{find_quorum, Quorum, QuorumType, QuorumVote};
use crate::IRStorage;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cluster size is 2f+1, as per page 4 of the extended paper (3.1.2 IR Guarantees)
/// Minimum cluster size of f=1 is 3
const MINIMUM_CLUSTER_SIZE: usize = 3;

/// This is how many retries will happen in total
/// Since the network should be performing the retries, this is set to 0
/// And is only used to enforce an end cycle to the loop
const MAX_ATTEMPTS: u8 = 0;

/// The client used to interact with the IR cluster.
/// Addresses are provided via the view on the storage interface.
pub struct InconsistentReplicationClient<
    N: IRNetwork<I, M>,
    S: IRClientStorage<I, M>,
    I: NodeID,
    M: IRMessage,
> {
    network: Arc<N>,
    storage: S,
    client_id: I,
    sequence: AtomicU64,
    latest_view: View<I>,
    additional_nodes: RwLock<Vec<I>>,
    _a: PhantomData<M>,
}

struct ResponseIntermediary<I: NodeID, M: IRMessage> {
    node: I,
    message: M,
    view: View<I>,
}

impl<
        NET: IRNetwork<ID, MSG> + 'static,
        STO: IRClientStorage<ID, MSG> + 'static,
        ID: NodeID + 'static,
        MSG: IRMessage + 'static,
    > InconsistentReplicationClient<NET, STO, ID, MSG>
{
    pub async fn new(network: NET, storage: STO, client_id: ID) -> Self {
        let view = storage.recover_current_view().await;
        InconsistentReplicationClient {
            network: Arc::new(network),
            storage,
            client_id,
            sequence: AtomicU64::new(0),
            latest_view: view,
            additional_nodes: RwLock::new(Vec::new()),
            _a: PhantomData,
        }
    }

    /// Make an inconsistent request to the cluster
    /// Inconsistent requests happen in any order
    /// Conflict resolution is done by the client after receiving responses
    pub async fn invoke_inconsistent(&self, message: MSG) -> Result<MSG, &'static str> {
        let nodes = &self.latest_view.members;
        let nodes_len = nodes.len();

        if nodes_len < MINIMUM_CLUSTER_SIZE {
            return Err("Cluster size is too small");
        }

        // Initiate requests
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let network = self.network.clone();
        let client_id = self.client_id.clone();
        let mut responses = self
            .request_until_number_of_responses(
                &nodes,
                sequence,
                message,
                move |node, sequence, message, _| {
                    Box::pin({
                        let network = network.clone();
                        let client_id = client_id.clone();
                        async move {
                            let r = network
                                .propose_inconsistent(
                                    node.clone(),
                                    client_id,
                                    sequence,
                                    message,
                                    None,
                                )
                                .await;
                            (node, r.map_err(|_| "Failed to propose inconsistent"))
                        }
                    })
                },
            )
            .await?;
        let quorum: Quorum<ID, MSG> =
            find_quorum(responses.iter().map(|(node_id, (msg, view))| QuorumVote {
                node: node_id,
                message: msg,
                view,
            }))
            .map_err(|_| "Quorum not found")?;
        for node_id in quorum.view.members.iter().cloned() {
            self.network
                .async_finalize(
                    node_id,
                    self.client_id.clone(),
                    sequence,
                    quorum.message.clone(),
                )
                .await
                .unwrap();
        }
        Ok(quorum.message.clone())
    }

    /// Make a consistent request to the cluster
    /// Consistent requests happen in any order
    /// A provided function helps resolve conflicts once detected
    /// This same function is used during recovery
    pub async fn invoke_consistent<F: DecideFunction<MSG>>(
        &self,
        message: MSG,
        decide_function: F,
    ) -> Result<(), &'static str> {
        let current_view = self.storage.recover_current_view().await;
        let nodes = current_view.members;

        if nodes.len() < MINIMUM_CLUSTER_SIZE {
            return Err("Cluster size is too small");
        }

        // Initiate requests
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let network = self.network.clone();
        let client_id = self.client_id.clone();
        let responses = self
            .request_until_number_of_responses(
                &nodes,
                sequence,
                message,
                |node, sequence, message, _| {
                    Box::pin({
                        let network = network.clone();
                        let client_id = client_id.clone();
                        // Copy 🤡
                        let sequence = sequence;
                        async move {
                            let res = network
                                .propose_consistent(node.clone(), client_id, sequence, message)
                                .await;
                            (node, res.map_err(|_| "Failed to propose consistent"))
                        }
                    })
                },
            )
            .await?;
        let quorum = find_quorum(responses.iter().map(|(node_id, (msg, view))| QuorumVote {
            node: node_id,
            message: msg,
            view,
        }))
        .map_err(|_| "Quorum not found")?;

        match quorum.quorum_type {
            QuorumType::FastQuorum => {
                // We can do async finalize
                for node_id in quorum.view.members.iter().cloned() {
                    self.network
                        .async_finalize(
                            node_id,
                            self.client_id.clone(),
                            sequence,
                            quorum.message.clone(),
                        )
                        .await
                        .unwrap();
                }
            }
            QuorumType::NormalQuorum => {

                // TODO wait for f+1 confirm responses
            }
        }

        Ok(())
    }

    /// Use this function to make the client additionally make requests to these nodes
    /// Only nodes in the current view are considered for quorum, so this is a useful way to
    /// add nodes to the network.
    ///
    /// If the nodes respond with a normal view (i.e. caught up), a view change will be initiated.
    ///
    /// The list of additional nodes is cleared on view change.
    pub async fn add_nodes_to_probe(&self, nodes: Vec<ID>) {
        let mut additional_nodes = self.additional_nodes.write().await;
        additional_nodes.extend(nodes);
    }

    /// Given an iterable of views, and a provided expected view
    /// Return Ok if all views match (with the expected as value)
    /// Return Err if they don't match (with latest as value)
    fn validate_view<'a, I: IntoIterator<Item = &'a View<ID>>>(
        views: I,
        expected: Option<&'a View<ID>>,
    ) -> Result<&'a View<ID>, &'a View<ID>> {
        let mut iter = views.into_iter();
        let mut highest = expected.or(iter.next()).unwrap();
        let mut failed = false;
        while let Some(view) = iter.next() {
            if view.view > highest.view {
                highest = view;
                failed = true;
            } else if view.view < highest.view {
                failed = true;
            }
        }
        match failed {
            true => Err(highest),
            false => Ok(highest),
        }
    }

    async fn request_until_number_of_responses<
        F: Fn(
            ID,
            u64,
            MSG,
            Option<View<ID>>,
        ) -> Pin<Box<dyn Future<Output = (ID, Result<(MSG, View<ID>), &'static str>)>>>,
    >(
        &self,
        nodes: &[ID],
        sequence: u64,
        message: MSG,
        request: F,
    ) -> Result<Vec<(ID, (MSG, View<ID>))>, &'static str> {
        let mut requests = FuturesUnordered::new();
        for node in nodes {
            requests.push(request(node.clone(), sequence, message.clone(), None));
        }
        let mut responses = Vec::with_capacity(requests.len());

        let mut attempts = 0;
        // TODO remove the max attempts - the algo should not be handling network failures. The network layer does retries.
        while responses.len() < nodes.len() {
            match requests.next().await {
                Some((node, Ok(response))) => responses.push((node, response)),
                Some((node, Err(_e))) => {
                    // Retry the request
                    if attempts < MAX_ATTEMPTS {
                        attempts += 1;
                        requests.push(request(node, sequence, message.clone(), None));
                    }
                }
                None => break,
            }
        }
        Ok(responses)
    }

    /// This helper is required so that we have a consistently-sized future
    async fn propose_inconsistent_helper(
        &self,
        node: ID,
        sequence: u64,
        message: MSG,
        highest_observed_view: Option<View<ID>>,
    ) -> (ID, Result<(MSG, View<ID>), ()>) {
        let response = self
            .network
            .propose_inconsistent(
                node.clone(),
                self.client_id.clone(),
                sequence,
                message,
                highest_observed_view,
            )
            .await;
        (node, response)
    }
}

#[cfg(test)]
mod test {
    use crate::client::InconsistentReplicationClient;
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};
    use crate::types::{IRMessage, NodeID};
    use crate::InconsistentReplicationServer;

    #[tokio::test]
    async fn client_can_make_inconsistent_requests() {
        // given a cluster
        let network = MockIRNetwork::<_, _, MockIRStorage<_, _>>::new();
        let members = vec![1, 2, 3];
        let storage = MockIRStorage::new(members.clone());
        mock_cluster(&network, members).await;

        // and a client
        let client = InconsistentReplicationClient::new(network.clone(), storage, 0).await;

        // when the client makes a request
        let result = client.invoke_inconsistent(&[4, 5, 6]).await;

        // then the request is handled
        assert!(result.is_ok(), "{:?}", result);
    }

    #[tokio::test]
    async fn client_fails_inconsistent_request_no_quorum() {
        // given a cluster
        let network = MockIRNetwork::<_, _, MockIRStorage<_, _>>::new();
        let members = vec![1, 2, 3];
        let storage = MockIRStorage::new(members.clone());
        mock_cluster(&network, members).await;

        // and a client
        let client = InconsistentReplicationClient::new(network.clone(), storage, 0).await;

        // when we prevent the request from being sent
        network.drop_requests_add(1, 2000);
        network.drop_requests_add(2, 2000);

        // and we make the client perform the requests
        let result = client.invoke_inconsistent(&[4, 5, 6]).await;

        // then the request was handled
        assert!(result.is_err());
    }

    async fn mock_cluster<ID: NodeID, MSG: IRMessage>(
        network: &MockIRNetwork<ID, MSG, MockIRStorage<ID, MSG>>,
        nodes: Vec<ID>,
    ) {
        for node_id in &nodes {
            network.register_node(
                node_id.clone(),
                InconsistentReplicationServer::new(
                    network.clone(),
                    MockIRStorage::new(nodes.clone()),
                    node_id.clone(),
                )
                .await,
            );
        }
    }
}
