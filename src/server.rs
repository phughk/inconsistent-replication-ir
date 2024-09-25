use crate::io::{IRNetwork, IRStorage};
use crate::types::{IRMessage, NodeID};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

/// Implementation of a server node for receiving and handling operations according to the
/// Inconsistent Replication algorithm.
pub struct InconsistentReplicationServer<
    NET: IRNetwork<ID, MSG>,
    STO: IRStorage<ID, MSG>,
    ID: NodeID,
    MSG: IRMessage,
> {
    network: NET,
    storage: Arc<STO>,
    node_id: ID,
    view: ViewState,
    _a: PhantomData<MSG>,
}

impl<N, S, I, M> Clone for InconsistentReplicationServer<N, S, I, M>
where
    N: IRNetwork<I, M> + Clone,
    S: IRStorage<I, M> + Clone,
    I: NodeID,
    M: IRMessage,
{
    fn clone(&self) -> Self {
        InconsistentReplicationServer {
            network: self.network.clone(),
            storage: self.storage.clone(),
            node_id: self.node_id.clone(),
            view: self.view.clone(),
            _a: PhantomData,
        }
    }
}

///
#[derive(Clone, Eq, PartialEq)]
#[cfg_attr(test, derive(Debug))]
pub enum ViewState {
    Normal { view: u64 },
    ViewChanging { view: u64 },
    Recovery { view: u64 },
}

impl<
        N: IRNetwork<I, M> + 'static,
        S: IRStorage<I, M> + 'static,
        I: NodeID + 'static,
        M: IRMessage + 'static,
    > InconsistentReplicationServer<N, S, I, M>
{
    pub async fn new(network: N, storage: S, node_id: I) -> Self {
        let view = storage.recover_current_view().await;
        InconsistentReplicationServer {
            network,
            storage: Arc::new(storage),
            node_id,
            view,
            _a: PhantomData,
        }
    }

    pub fn exec_inconsistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = (M, ViewState)>>> {
        let storage = self.storage.clone();
        // TODO maybe read lock?
        let view = self.view.clone();
        Box::pin(async move {
            let m = storage
                .record_tentative(client_id, operation_sequence, message)
                .await;
            (m, view)
        })
    }

    pub fn exec_consistent(&self, _message: M) -> Pin<Box<dyn Future<Output = (M, ViewState)>>> {
        unimplemented!("Implement me!");
    }
}

#[cfg(test)]
mod test {
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};
    use crate::server::{InconsistentReplicationServer, ViewState};
    use std::sync::Arc;

    #[tokio::test]
    pub async fn recovers_view() {
        // when
        let network = MockIRNetwork::<Arc<String>, Arc<String>, MockIRStorage<_, _>>::new();
        let storage = MockIRStorage::new();
        storage
            .set_current_view(ViewState::Normal { view: 0 })
            .await;

        let server =
            InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()))
                .await;
        network.register_node(Arc::new("1".to_string()), server.clone());
        assert_eq!(&server.view, &ViewState::Normal { view: 0 });
    }
}
