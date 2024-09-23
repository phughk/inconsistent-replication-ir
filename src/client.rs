use std::sync::OnceState;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use crate::io::{ClientStorage, IRNetwork};
use crate::types::{IRMessage, NodeID};

pub struct InconsistentReplicationClient<N: IRNetwork<I, M>, S: ClientStorage, I: NodeID, M: IRMessage> {
    network: N,
    storage: S,
    client_id: I,
}

impl <NET: IRNetwork<ID, MSG>, STO: ClientStorage, ID: NodeID, MSG: IRMessage> InconsistentReplicationClient<NET, STO, ID, MSG> {
    pub fn new(network: NET, storage: STO, client_id: ID) -> Self {
        InconsistentReplicationClient { network, storage, client_id }
    }

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
        let fast_quorum = 3*f/2 + 1;
        for _ in 0..fast_quorum {
            match requests.next().await {
                Some(response) => responses.push(response),
                None => break,
            }
        }
        let enough_responses = responses.len() >= fast_quorum;
        let all_same = responses.iter().all(|r| r == responses[0]);
        if enough_responses && all_same {
            return;
        }

        // We do not have a fast quorum and must continue to a slow quorum

    }

    pub fn invoke_consistent(&self){

    }
}

#[cfg(test)]
mod test {
    use crate::client::InconsistentReplicationClient;
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};

    fn client_can_make_inconsistent_requests() {
        let network = MockIRNetwork {};
        let storage = MockIRStorage {};
        let client = InconsistentReplicationClient::new(network, storage, "1");
        client.
    }
}