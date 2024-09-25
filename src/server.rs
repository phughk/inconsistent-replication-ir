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
#[cfg_attr(any(test, debug_assertions), derive(Debug))]
pub enum ViewState {
    Normal { view: u64 },
    ViewChanging { view: u64 },
    Recovery { view: u64 },
}

impl ViewState {
    pub fn view(&self) -> u64 {
        match self {
            ViewState::Normal { view } => *view,
            ViewState::ViewChanging { view } => *view,
            ViewState::Recovery { view } => *view,
        }
    }
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

    /// Invoked on propose message
    pub fn propose_inconsistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
        highest_observed_view: Option<ViewState>,
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

    /// Invoked on finalize message
    pub fn exec_inconsistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = (M, ViewState)>>> {
        let storage = self.storage.clone();
        let view = self.view.clone();
        Box::pin(async move {
            let _ = storage
                .promote_finalized_and_run(client_id, operation_sequence)
                .await;
            (message, view)
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
    pub async fn recovers_view_from_storage() {
        // when
        let network = MockIRNetwork::<Arc<String>, Arc<String>, MockIRStorage<_, _>>::new();
        let storage = MockIRStorage::new();
        storage
            .set_current_view(ViewState::Normal { view: 3 })
            .await;

        let server =
            InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()))
                .await;
        network.register_node(Arc::new("1".to_string()), server.clone());
        assert_eq!(&server.view, &ViewState::Normal { view: 3 });
    }

    #[tokio::test]
    pub async fn changes_view_on_higher_value_propose_inconsistent() {
        let network = MockIRNetwork::<Arc<String>, Arc<String>, MockIRStorage<_, _>>::new();
        let storage = MockIRStorage::new();
        storage
            .set_current_view(ViewState::Normal { view: 3 })
            .await;

        let server =
            InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()));
        let new_view = ViewState::Normal { view: 4 };
        server.await.propose_inconsistent(
            Arc::new("1".to_string()),
            1,
            Arc::new("msg".to_string()),
        );
    }
}
