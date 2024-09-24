use crate::io::IRNetwork;
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Cluster size is 2f+1, as per page 4 of the extended paper (3.1.2 IR Guarantees)
/// Minimum cluster size of f=1 is 3
const MINIMUM_CLUSTER_SIZE: usize = 3;

pub struct InconsistentReplicationClient<
    N: IRNetwork<I, M>,
    S: IRStorage<I, M>,
    I: NodeID,
    M: IRMessage,
> {
    network: N,
    #[allow(unused)]
    storage: S,
    client_id: I,
    sequence: AtomicUsize,
    _a: PhantomData<M>,
}

impl<NET: IRNetwork<ID, MSG>, STO: IRStorage<ID, MSG>, ID: NodeID, MSG: IRMessage>
    InconsistentReplicationClient<NET, STO, ID, MSG>
{
    pub fn new(network: NET, storage: STO, client_id: ID) -> Self {
        InconsistentReplicationClient {
            network,
            storage,
            client_id,
            sequence: AtomicUsize::new(0),
            _a: PhantomData,
        }
    }

    /// Make an inconsistent request to the cluster
    /// Inconsistent requests happen in any order
    /// Conflict resolution is done by the client after receiving responses
    pub async fn invoke_inconsistent(&self, message: MSG) -> Result<(), &'static str> {
        let nodes = self.network.get_members().await;

        if nodes.len() < MINIMUM_CLUSTER_SIZE {
            return Err("Cluster size is too small");
        }
        // Derive f, assuming cluster size is 3f+1
        let f = (nodes.len() - 1) / 3;

        // Initiate requests
        let mut requests = FuturesUnordered::new();
        for node in nodes {
            requests.push(self.network.request_inconsistent(
                node,
                self.client_id.clone(),
                self.sequence.fetch_add(1, Ordering::SeqCst),
                message.clone(),
            ));
        }
        let mut responses = Vec::with_capacity(requests.len());

        // Try fast quorum of 3f/2+1 with all responses the same
        let fast_quorum = 3 * f / 2 + 1;
        for _ in 0..fast_quorum {
            match requests.next().await {
                Some(response) => responses.push(response),
                None => break,
            }
        }
        if responses.is_empty() {
            return Err("No responses received");
        }
        let enough_responses = responses.len() >= fast_quorum;
        let all_same = responses.iter().all(|r| r == &responses[0]);
        if enough_responses && all_same && responses[0].is_ok() {
            return Ok(());
        }

        // We do not have a fast quorum and must continue to a slow quorum
        Err("Slow quorum is unimplemented")
    }

    /// Make a consistent request to the cluster
    /// Consistent requests happen in any order
    /// A provided function helps resolve conflicts once detected
    /// This same function is used during recovery
    pub fn invoke_consistent(&self) {
        unimplemented!("Implement me!");
    }
}

#[cfg(test)]
mod test {
    use crate::client::InconsistentReplicationClient;
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};
    use crate::types::{IRMessage, Incrementable, NodeID};
    use crate::InconsistentReplicationServer;

    #[tokio::test]
    async fn client_can_make_inconsistent_requests() {
        // given a cluster
        let network = MockIRNetwork::<_, _, MockIRStorage<_, _>>::new();
        let storage = MockIRStorage::new();
        mock_cluster(&network, &[1, 2, 3, 4]).await;

        // and a client
        let client = InconsistentReplicationClient::new(network.clone(), storage, 0);

        // when the client makes a request
        let a = client.invoke_inconsistent(&[4, 5, 6]).await;

        // then the request is handled
        assert!(a.is_ok());
    }

    #[tokio::test]
    async fn client_fails_inconsistent_request_no_quorum() {
        // given a cluster
        let network = MockIRNetwork::<_, _, MockIRStorage<_, _>>::new();
        let storage = MockIRStorage::new();
        mock_cluster(&network, &[1, 2, 3, 4]).await;

        // and a client
        let client = InconsistentReplicationClient::new(network.clone(), storage, 0);

        // when we prevent the request from being sent
        network.drop_packets_add(1, 1);
        network.drop_packets_add(2, 1);

        // and we make the client perform the requests
        let a = client.invoke_inconsistent(&[4, 5, 6]).await;

        // then the request was handled
        assert!(a.is_err());
    }

    async fn mock_cluster<ID: NodeID + Incrementable, MSG: IRMessage>(
        network: &MockIRNetwork<ID, MSG, MockIRStorage<ID, MSG>>,
        nodes: &[ID],
    ) {
        for node_id in nodes {
            network.register_node(
                node_id.clone(),
                InconsistentReplicationServer::new(
                    network.clone(),
                    MockIRStorage::new(),
                    node_id.clone(),
                ),
            );
        }
    }
}
