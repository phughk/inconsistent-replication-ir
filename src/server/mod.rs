#[cfg(test)]
mod test;

use crate::debug::MaybeDebug;
use crate::io::{IRNetwork, IRStorage};
use crate::types::{IRMessage, NodeID, OperationSequence};
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
    storage: STO,
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
            storage,
            node_id,
            view: Arc::new(RwLock::new(view)),
            _a: PhantomData,
        }
    }

    /// Invoked on propose message
    pub fn propose_inconsistent(
        &self,
        client_id: I,
        operation_sequence: OperationSequence,
        message: M,
        // TODO
        _highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = (M, View<I>)>>> {
        #[cfg(any(feature = "test", test))]
        println!(
            "propose_inconsistent: {}",
            MaybeDebug::maybe_debug(&message)
        );
        let storage = self.storage.clone();
        let view = self.view.clone();
        Box::pin(async move {
            let view_lock = view.read().await;
            let view = view_lock.clone();
            assert_eq!(view.state, ViewState::Normal);
            let m = storage
                .record_tentative_inconsistent_and_evaluate(
                    client_id,
                    operation_sequence,
                    view.clone(),
                    message,
                )
                .await;
            (m, view)
        })
    }

    /// Invoked on finalize message
    pub fn finalize_inconsistent(
        &self,
        client_id: I,
        operation_sequence: OperationSequence,
        message: M,
    ) -> Pin<Box<dyn Future<Output = (M, View<I>)>>> {
        #[cfg(any(feature = "test", test))]
        println!(
            "finalize_inconsistent: {}",
            MaybeDebug::maybe_debug(&message)
        );
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
        operation_sequence: OperationSequence,
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
        operation_sequence: OperationSequence,
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

    #[cfg(any(feature = "test", test))]
    pub async fn shutdown(self) -> (N, S, I, View<I>) {
        let view_guard = self.view.write().await;
        let view = view_guard.to_owned();
        (self.network, self.storage, self.node_id, view)
    }

    async fn do_view_change(&self, new_view: View<I>) {
        assert_eq!(new_view.state, ViewState::ViewChanging);
        let old_view = self.view.write().await;
        assert!(new_view.view > old_view.view);
    }
}
