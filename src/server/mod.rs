#[cfg(test)]
mod test;

use crate::debug::MaybeDebug;
use crate::io::{IRNetwork, IRStorage};
use crate::types::{AsyncIterator, IRMessage, NodeID, OperationSequence};
use crate::utils::f;
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
        let mut view = storage.recover_current_view().await;
        view.state = ViewState::Recovery;
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
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), IRServerError<I>>>>> {
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
            Ok((m, view))
        })
    }

    /// Invoked on finalize message
    pub fn finalize_inconsistent(
        &self,
        client_id: I,
        operation_sequence: OperationSequence,
        message: M,
        // TODO
        _highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), IRServerError<I>>>>> {
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
            Ok((message, view))
        })
    }

    /// Proposes a consistent operation
    pub fn propose_consistent(
        &self,
        client_id: I,
        operation_sequence: OperationSequence,
        message: M,
        // TODO
        _highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), IRServerError<I>>>>> {
        let view = self.view.clone();
        let storage = self.storage.clone();
        Box::pin(async move {
            let view_lock = view.read().await;
            let view = view_lock.clone();
            if view.state == ViewState::Recovery {
                return Err(IRServerError::Recovering(view));
            }
            let resolved_message = storage
                .record_tentative_and_exec_consistent(
                    client_id,
                    operation_sequence,
                    view.clone(),
                    message,
                )
                .await;
            Ok((resolved_message, view))
        })
    }

    /// Finalize and execute a consistent operation
    pub fn finalize_consistent(
        &self,
        client_id: I,
        operation_sequence: OperationSequence,
        message: M,
        // TODO
        _highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), IRServerError<I>>>>> {
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
            Ok((m, view))
        })
    }

    /// Invoked when another node in the cluster is sending its operations.
    /// The actual implementation includes self records, so you can do optimisations behind
    /// the scenes, such as passively uploading, or tracking which operations already exist on
    /// the leader node (this node).
    pub async fn process_incoming_operations<ITER: AsyncIterator<Item = IROperation<I, M>>>(
        &self,
        from_who: I,
        view: View<I>,
        operations: ITER,
    ) {
        while let Some(operation) = operations.next().await {
            self.storage
                .track_view_operation(from_who.clone(), view.clone(), operation)
                .await;
        }
        let view = self.view.read().await;
        let full_records = self.storage.full_records_received(view.clone()).await;
        // if we have f+1 full records we can start merge
        if full_records.len() >= f(view.members.len()).unwrap() + 1 {
            self.merge(full_records, view.clone()).await;
        }
    }

    async fn merge(&self, full_record_members: Vec<I>, view: View<I>) {
        for node in full_record_members {
            let ops_iter = self.storage.get_view_record_operations(node, view.clone());
            for op in ops_iter {
                let existing_main_record_op = self.storage.get_main_or_local_operation()
            }
        }
    }

    /// This method should be run in a loop from within the server, as it handles recovery etc
    pub async fn perform_maintenance(&self) {}

    #[cfg(any(feature = "test", test))]
    pub async fn shutdown(self) -> (N, S, I, View<I>) {
        let view_guard = self.view.write().await;
        let view = view_guard.to_owned();
        (self.network, self.storage, self.node_id, view)
    }
}

#[derive(Debug)]
pub enum IRServerError<ID: NodeID> {
    InternalError(Box<dyn std::error::Error>),
    Recovering(View<ID>),
}

pub enum IROperation<ID: NodeID, MSG: IRMessage> {
    InconsistentPropose {
        client: ID,
        sequence: OperationSequence,
        message: MSG,
    },
    InconsistentFinalize {
        client: ID,
        sequence: OperationSequence,
        message: MSG,
    },
    ConsistentPropose {
        client: ID,
        sequence: OperationSequence,
        message: MSG,
    },
    ConsistentFinalize {
        client: ID,
        sequence: OperationSequence,
        message: MSG,
    },
}

impl <ID: NodeID, MSG: IRMessage> IROperation<ID, MSG> {
    pub fn client(&self) -> &ID {
        match self {
            IROperation::InconsistentPropose { client, ..} => client,
            IROperation::InconsistentFinalize { client, ..} => client,
            IROperation::ConsistentPropose { client,..} => client,
            IROperation::ConsistentFinalize { client,..} => client
        }
    }

    pub fn sequence(&self) -> &OperationSequence {
        match self {
            IROperation::InconsistentPropose { sequence,..} => sequence,
            IROperation::InconsistentFinalize { sequence,..} => sequence,
            IROperation::ConsistentPropose { sequence,..} => sequence,
            IROperation::ConsistentFinalize { sequence,..} => sequence,
        }
    }
}
