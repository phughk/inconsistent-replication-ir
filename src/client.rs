use crate::io::{ClientStorage, IRNetwork};
use crate::types::{IRMessage, NodeID};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::marker::PhantomData;
use std::sync::OnceState;

pub struct InconsistentReplicationClient<
    N: IRNetwork<I, M>,
    S: ClientStorage,
    I: NodeID,
    M: IRMessage,
> {
    network: N,
    storage: S,
    client_id: I,
    _a: PhantomData<M>,
}

impl<NET: IRNetwork<ID, MSG>, STO: ClientStorage, ID: NodeID, MSG: IRMessage>
    InconsistentReplicationClient<NET, STO, ID, MSG>
{
    pub fn new(network: NET, storage: STO, client_id: ID) -> Self {
        InconsistentReplicationClient {
            network,
            storage,
            client_id,
            _a: PhantomData,
        }
    }

    /// Make an inconsistent request to the cluster
    /// Inconsistent requests happen in any order
    /// Conflict resolution is done by the client after receiving responses
    pub async fn invoke_inconsistent(&self, message: MSG) {
        let nodes = self.network.get_members().await;

        // Derive f, assuming cluster size is 3f+1
        let f = (nodes.len() - 1) / 3;

        // Initiate requests
        let mut requests = FuturesUnordered::new();
        for node in nodes {
            requests.push(self.network.request_inconsistent(node, message.clone()));
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
            panic!("No responses received");
        }
        let enough_responses = responses.len() >= fast_quorum;
        let all_same = responses.iter().all(|r| r == &responses[0]);
        if enough_responses && all_same && responses[0].is_ok() {
            return;
        }

        // We do not have a fast quorum and must continue to a slow quorum
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

    #[tokio::test]
    async fn client_can_make_inconsistent_requests() {
        let network = MockIRNetwork {};
        let storage = MockIRStorage {};
        let client = InconsistentReplicationClient::new(network, storage, "1");
        client.invoke_inconsistent("Hello, world!").await;
    }
}
