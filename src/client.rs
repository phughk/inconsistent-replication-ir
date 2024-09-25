use crate::io::{IRClientStorage, IRNetwork};
use crate::server::View;
use crate::types::{DecideFunction, IRMessage, NodeID};
use crate::IRStorage;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// Cluster size is 2f+1, as per page 4 of the extended paper (3.1.2 IR Guarantees)
/// Minimum cluster size of f=1 is 3
const MINIMUM_CLUSTER_SIZE: usize = 3;

/// This is how many retries will happen in total
const MAX_ATTEMPTS: u8 = 100;

/// Derive f (number of tolerable failures) from the number of nodes in the cluster
const fn f(nodes: usize) -> usize {
    (nodes - 1) / 2
}

/// Derive the fast quorum size from the number of nodes in the cluster
const fn fast_quorum(nodes: usize) -> usize {
    3 * f(nodes) / 2 + 1
}

/// Derive the normal quorum size from the number of nodes in the cluster
const fn slow_quorum(nodes: usize) -> usize {
    f(nodes) + 1
}

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

impl<NET: IRNetwork<ID, MSG>+'static, STO: IRClientStorage<ID, MSG>+'static, ID: NodeID+'static, MSG: IRMessage+'static>
    InconsistentReplicationClient<NET, STO, ID, MSG>
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
            .request_until_quorum(&nodes, sequence, message, move |node, sequence, message, _| {
                Box::pin({
                    let network = network.clone();
                    let client_id = client_id.clone();
                    async move {
                        let r = network.propose_inconsistent(node.clone(), client_id, sequence, message, None).await;
                        (node, r.map_err(|_| "Failed to propose inconsistent"))
                    }
                })
            })
            .await?;
        assert!(responses.len() >= slow_quorum(nodes_len));

        // Validate views
        if let Err(highest) = Self::validate_view(responses.iter().map(|r| &r.1 .1), None) {
            // Views are invalid and we must now message invalid views to catch up
            for (node, (_msg, view)) in &responses {
                if view.view < highest.view {
                    self.network
                        .invoke_view_change(node.clone(), highest.clone())
                        .await
                        .unwrap();
                }
            }
            return Err("Views are invalid");
        }

        if responses.is_empty() {
            return Err("No responses received");
        }
        let expected_msg = &responses[0].1 .0;
        let all_same = responses.iter().all(|r| {
            let (_, (msg, _)) = r;
            msg == expected_msg
        });
        if all_same {
            let (_node, (msg, _view)) = responses.into_iter().next().unwrap();
            return Ok(msg);
        }
        Err("Responses didn't match or weren't enough")
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
            .request_until_quorum(&nodes, sequence, message, |node, sequence, message, _| {
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
            })
            .await?;
        assert!(responses.len() >= slow_quorum(nodes.len()));

        let all_same = responses.iter().all(|r| r == &responses[0]);
        if all_same {
            for node in nodes {
                let (_node_id, (msg, view)) = responses[0].clone();
                self.network
                    .async_finalize(node, self.client_id.clone(), sequence, msg)
                    .await
                    .unwrap();
            }
            return Ok(());
        }

        // We do not have a fast quorum and must continue to a slow quorum
        let mut votes = BTreeSet::new();
        for (_node_id, (msg, _view)) in responses {
            votes.insert(msg);
        }
        // Now we let the decide function decide the result
        let result = decide_function.decide(votes.iter());
        // Finally send the decided vote to all nodes
        for node in nodes {
            self.network
                .sync_finalize(node, self.client_id.clone(), sequence, result.clone())
                .await
                .unwrap();
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

    async fn request_until_quorum<
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
        while responses.len() < slow_quorum(nodes.len()) && attempts < MAX_ATTEMPTS {
            match requests.next().await {
                Some((node, Ok(response))) => responses.push((node, response)),
                Some((node, Err(_e))) => {
                    // Retry the request
                    attempts += 1;
                    requests.push(request(node, sequence, message.clone(), None));
                }
                None => break,
            }
        }
        if attempts >= MAX_ATTEMPTS {
            return Err("Too many attempts");
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

    #[test]
    fn test_f() {
        assert_eq!(super::f(1), 0);
        assert_eq!(super::f(2), 0);
        assert_eq!(super::f(3), 1);
        assert_eq!(super::f(4), 1);
        assert_eq!(super::f(5), 2);
        assert_eq!(super::f(6), 2);
        assert_eq!(super::f(7), 3);
    }

    #[test]
    fn test_fast_quorum() {
        assert_eq!(super::fast_quorum(3), 2);
        assert_eq!(super::fast_quorum(4), 2);
        assert_eq!(super::fast_quorum(5), 4);
        assert_eq!(super::fast_quorum(6), 4);
        assert_eq!(super::fast_quorum(7), 5);
    }

    #[test]
    fn test_slow_quorum() {
        assert_eq!(super::slow_quorum(3), 2);
        assert_eq!(super::slow_quorum(4), 2);
        assert_eq!(super::slow_quorum(5), 3);
        assert_eq!(super::slow_quorum(6), 3);
        assert_eq!(super::slow_quorum(7), 4);
    }

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
