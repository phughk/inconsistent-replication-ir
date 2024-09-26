use crate::io::{IRNetwork, IRStorage};
use crate::types::{IRMessage, NodeID};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    view: Arc<RwLock<View<ID>>>,
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
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(any(test, debug_assertions), derive(Debug))]
pub struct View<ID: NodeID> {
    pub view: u64,
    pub members: Vec<ID>,
    pub state: ViewState,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(any(test, debug_assertions), derive(Debug))]
pub enum ViewState {
    Normal,
    ViewChanging,
    Recovery,
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
            view: Arc::new(RwLock::new(view)),
            _a: PhantomData,
        }
    }

    /// Invoked on propose message
    pub fn propose_inconsistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
        // TODO
        _highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = (M, View<I>)>>> {
        let storage = self.storage.clone();
        let view = self.view.clone();
        Box::pin(async move {
            let view_lock = view.read().await;
            let view = view_lock.clone();
            assert_eq!(view.state, ViewState::Normal);
            let m = storage
                .record_tentative_inconsistent(client_id, operation_sequence, view.clone(), message)
                .await;
            (m, view)
        })
    }

    /// Invoked on finalize message
    pub fn finalize_inconsistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = (M, View<I>)>>> {
        let storage = self.storage.clone();
        let view = self.view.clone();
        Box::pin(async move {
            let view_lock = view.read().await;
            let view = view_lock.clone();
            assert_eq!(view.state, ViewState::Normal);
            let _ = storage
                .promote_finalized_and_exec_inconsistent(
                    client_id,
                    operation_sequence,
                    view.clone(),
                    message.clone(),
                )
                .await;
            (message, view)
        })
    }

    /// Proposes a consistent operation
    pub fn propose_consistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = (M, View<I>)>>> {
        let view = self.view.clone();
        let storage = self.storage.clone();
        Box::pin(async move {
            let view_lock = view.read().await;
            let view = view_lock.clone();
            assert_eq!(view.state, ViewState::Normal);
            let resolved_message = storage
                .record_tentative_and_exec_consistent(
                    client_id,
                    operation_sequence,
                    view.clone(),
                    message,
                )
                .await;
            (resolved_message, view)
        })
    }

    /// Finalize and execute a consistent operation
    pub fn finalize_consistent(
        &self,
        client_id: I,
        operation_sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = (M, View<I>)>>> {
        let view = self.view.clone();
        let storage = self.storage.clone();
        Box::pin(async move {
            let view_lock = view.read().await;
            let view = view_lock.clone();
            assert_eq!(view.state, ViewState::Normal);
            let m = storage
                .promote_finalized_and_reconcile_consistent(
                    client_id,
                    operation_sequence,
                    view.clone(),
                    message,
                )
                .await;
            (m, view)
        })
    }

    async fn do_view_change(&self, new_view: View<I>) {
        assert_eq!(new_view.state, ViewState::ViewChanging);
        let old_view = self.view.write().await;
        assert!(new_view.view > old_view.view);
    }
}

#[cfg(test)]
mod test {
    use crate::io::test_utils::{MockIRNetwork, MockIRStorage};
    use crate::server::{InconsistentReplicationServer, View, ViewState};
    use crate::test_utils::mock_computers::NoopComputer;
    use std::sync::Arc;

    #[tokio::test]
    pub async fn recovers_view_from_storage() {
        // when
        let network = MockIRNetwork::<Arc<String>, Arc<String>, MockIRStorage<_, _, _>>::new();
        let members = vec![
            Arc::new("1".to_string()),
            Arc::new("2".to_string()),
            Arc::new("3".to_string()),
        ];
        let storage = MockIRStorage::new(members.clone(), NoopComputer::new());
        storage
            .set_current_view(View {
                view: 3,
                members: members.clone(),
                state: ViewState::Normal,
            })
            .await;

        let server =
            InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()))
                .await;
        network.register_node(Arc::new("1".to_string()), server.clone());
        let lock = server.view.read().await;
        assert_eq!(
            &*lock,
            &View {
                view: 3,
                members: members.clone(),
                state: ViewState::Normal
            }
        );
    }

    #[tokio::test]
    pub async fn changes_view_on_higher_value_propose_inconsistent() {
        let network = MockIRNetwork::<
            Arc<String>,
            Arc<String>,
            MockIRStorage<_, _, NoopComputer<Arc<String>>>,
        >::new();
        let members = vec![
            Arc::new("1".to_string()),
            Arc::new("2".to_string()),
            Arc::new("3".to_string()),
        ];
        let storage = MockIRStorage::new(members.clone(), NoopComputer::new());
        storage
            .set_current_view(View {
                view: 3,
                members: members.clone(),
                state: ViewState::Normal,
            })
            .await;

        let server =
            InconsistentReplicationServer::new(network.clone(), storage, Arc::new("1".to_string()));
        let new_view = View::<Arc<String>> {
            view: 4,
            members: vec![],
            state: ViewState::Normal,
        };
        server.await.propose_inconsistent(
            Arc::new("1".to_string()),
            1,
            Arc::new("msg".to_string()),
            None,
        );
    }
}
