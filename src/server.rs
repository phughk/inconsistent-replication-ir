use crate::io::{IRNetwork, IRStorage};
use crate::types::{IRMessage, NodeID};
use std::marker::PhantomData;

/// Implementation of a server node for receiving and handling operations according to the
/// Inconsistent Replication algorithm.
pub struct InconsistentReplicationServer<
    N: IRNetwork<I, M>,
    S: IRStorage<I, M>,
    I: NodeID,
    M: IRMessage,
> {
    network: N,
    storage: S,
    node_id: I,
    view: ViewState,
    _a: PhantomData<M>,
}

///
enum ViewState {
    Normal { view: usize },
    ViewChanging { view: usize },
    Recovery { view: usize },
}

impl<N: IRNetwork<I, M>, S: IRStorage<I, M>, I: NodeID, M: IRMessage>
    InconsistentReplicationServer<N, S, I, M>
{
    pub fn new(network: N, storage: S, node_id: I) -> Self {
        InconsistentReplicationServer {
            network,
            storage,
            node_id,
            view: ViewState::Recovery { view: 0 },
            _a: PhantomData,
        }
    }

    pub fn exec_inconsistent(&self, message: M) {
        unimplemented!("Implement me!");
    }

    pub fn exec_consistent(&self, message: M) {
        unimplemented!("Implement me!");
    }
}

#[cfg(test)]
mod test {
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};
    use crate::server::InconsistentReplicationServer;

    #[tokio::test]
    pub fn starts_in_view_zero() {
        // when
        let network = MockIRNetwork {};
        let storage = MockIRStorage {};

        let server = InconsistentReplicationServer::new(network, storage, "1");
        // server.get_view();
        unimplemented!("Implement me!");
    }
}
