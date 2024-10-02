use crate::debug::MaybeDebug;
use crate::io::{IRClientStorage, StorageShared};
use crate::server::{IROperation, View, ViewState};
use crate::test_utils::mock_computers::MockOperationHandler;
use crate::test_utils::mock_record_store::{FullState, MockRecordStore};
use crate::types::{AsyncIterator, IRMessage, NodeID, OperationSequence};
use crate::IRStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as TokioRwLock;

#[derive(Clone)]
pub struct FakeIRStorage<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> {
    /// Stores the local record store
    records: MockRecordStore<ID, MSG>,
    /// Stores received records from nodes during view change. Can be purged once a view change completes.
    received_record_logs: Arc<RwLock<BTreeMap<(View<ID>, ID), MockRecordStore<ID, MSG>>>>,
    /// Just a tracker for local view in case of restart
    current_view: Arc<TokioRwLock<View<ID>>>,
    /// That thang that handles operation processing
    computer_lol: CPU,
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> StorageShared<ID>
    for FakeIRStorage<ID, MSG, CPU>
{
    fn recover_current_view(&self) -> Pin<Box<dyn Future<Output = View<ID>>>> {
        let view = self.current_view.clone();
        Box::pin(async move { view.read().await.clone() })
    }
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> IRStorage<ID, MSG>
    for FakeIRStorage<ID, MSG, CPU>
{
    fn record_tentative_inconsistent_and_evaluate(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        println!(
            "record_tentative_inconsistent operation: {}",
            MaybeDebug::maybe_debug(&message)
        );
        let records = self.records.clone();
        let computer_lol = self.computer_lol.clone();
        Box::pin(async move {
            let existing = records.find_entry(client.clone(), operation).await;
            match existing {
                None => {
                    // This is valid, inconsistent may not have been received
                }
                Some(state) => {
                    assert!(state.view == view);
                    assert!(state.ir_operation.message() == &message);
                    #[cfg(any(feature = "test", debug_assertions, test))]
                    match state.ir_operation {
                        IROperation::InconsistentPropose { .. } => {}
                        _ => panic!("invalid type"),
                    }
                }
            }
            // TODO if finalized, should return finalized value and that it is finalized
            records
                .propose_tentative_inconsistent(client, operation, view, message.clone())
                .await;
            computer_lol.evaluate_inconsistent(message)
        })
    }

    fn promote_finalized_and_exec_inconsistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        println!(
            "promote_finalized_and_exec_inconsistent: {}",
            MaybeDebug::maybe_debug(&message)
        );
        let records = self.records.clone();
        let computer = self.computer_lol.clone();
        Box::pin(async move {
            let existing = records.find_entry(client.clone(), operation).await;
            match existing {
                None => {
                    // This is valid, we may have missed the inconsistent message
                }
                Some(state) => {
                    assert_eq!(state.view, view);
                    // We do not assert message, as it may be different
                    match state.ir_operation {
                        IROperation::InconsistentFinalize { .. } => {}
                        _ => panic!("invalid type"),
                    }
                }
            }
            records
                .promote_finalized_inconsistent(client, operation, view, message.clone())
                .await;
            let _unused_msg = computer.exec_inconsistent(message);
        })
    }

    fn record_tentative_and_exec_consistent(
        &self,
        client: ID,
        sequence: u64,
        view: View<ID>,
        operation: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        let records = self.records.clone();
        let computer = self.computer_lol.clone();
        Box::pin(async move {
            let existing = records.find_entry(client.clone(), sequence).await;
            match existing {
                None => {
                    // All good here
                }
                Some(state) => {
                    assert!(state.view == view);
                    match state.ir_operation {
                        IROperation::ConsistentFinalize { .. } => {}
                        _ => panic!("invalid type"),
                    }
                }
            }
            let response = computer.exec_consistent(operation.clone());
            records
                .propose_tentative_consistent(client, sequence, view, operation.clone())
                .await;
            response
        })
    }

    fn promote_finalized_and_reconcile_consistent(
        &self,
        client: ID,
        sequence: u64,
        view: View<ID>,
        operation: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        let records = self.records.clone();
        let computer = self.computer_lol.clone();
        Box::pin(async move {
            let existing = records.find_entry(client.clone(), sequence).await;
            match existing {
                None => {
                    // Valid
                }
                Some(state) => {
                    assert!(state.view == view);
                    match state.ir_operation {
                        IROperation::ConsistentFinalize { .. } => {}
                        _ => panic!("invalid type"),
                    }
                }
            }
            let previous = records
                .promote_finalized_consistent_returning_previous_evaluation(
                    client,
                    sequence,
                    view,
                    operation.clone(),
                )
                .await;
            computer.reconcile_consistent(previous, operation)
        })
    }

    fn add_peer_view_change_operation(
        &self,
        node_id: ID,
        view: View<ID>,
        operation: IROperation<ID, MSG>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(async move {
            let mut wl = self.received_record_logs.write().unwrap();
            let record_store = wl
                .entry((view.clone(), node_id))
                .or_insert_with(|| MockRecordStore::new());
            let found = record_store
                .find_entry(operation.client().clone(), operation.sequence().clone())
                .await;
            match (operation, found) {
                // Inconsistent finalize is always recorded
                (
                    IROperation::InconsistentFinalize {
                        client,
                        sequence,
                        message,
                    },
                    _,
                ) => {
                    record_store
                        .promote_finalized_inconsistent(client, sequence, view, message)
                        .await;
                }
                // Inconsistent propose is recorded if we don't have it
                (
                    IROperation::InconsistentPropose {
                        client,
                        sequence,
                        message,
                    },
                    None,
                ) => {
                    record_store
                        .propose_tentative_inconsistent(client, sequence, view, message)
                        .await;
                }
                // If we have a record then we keep it otherwise
                (
                    IROperation::InconsistentPropose { .. },
                    Some(FullState {
                        ir_operation: IROperation::InconsistentPropose { .. },
                        view,
                    }),
                ) => {
                    // Noop
                }
                // Consistent finalize is always recorded
                (
                    IROperation::ConsistentFinalize {
                        client,
                        sequence,
                        message,
                    },
                    _,
                ) => {
                    record_store
                        .propose_tentative_consistent(client, sequence, view, message)
                        .await;
                }
                // Consistent propose is recorded if we don't have it
                (
                    IROperation::ConsistentPropose {
                        client,
                        sequence,
                        message,
                    },
                    None,
                ) => {
                    record_store
                        .propose_tentative_consistent(client, sequence, view, message)
                        .await;
                }
                // If we have a consistent record then we keep it
                (
                    IROperation::ConsistentPropose { .. },
                    Some(FullState {
                        ir_operation: IROperation::ConsistentPropose { .. },
                        view,
                    }),
                ) => {
                    // Noop
                }
            }
        })
    }

    fn get_peers_with_full_records(
        &self,
        view: View<ID>,
    ) -> Pin<Box<dyn Future<Output = Vec<ID>> + 'static>> {
        todo!()
    }

    fn get_view_record_operations(
        &self,
        node: ID,
        view: View<ID>,
    ) -> impl AsyncIterator<Item = IROperation<ID, MSG>> {
        todo!()
    }

    fn get_main_or_local_operation(
        &self,
        view: View<ID>,
        client: ID,
        operation_sequence: OperationSequence,
    ) -> Option<IROperation<ID, MSG>> {
        todo!()
    }

    fn record_main_operation(&self, view: View<ID>, operation: IROperation<ID, MSG>) {
        todo!()
    }

    fn get_unresolved_record_operations(
        &self,
        view: View<ID>,
    ) -> Pin<Box<dyn Future<Output = impl AsyncIterator<Item = Vec<IROperation<ID, MSG>>>>>> {
        todo!()
    }
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> IRClientStorage<ID, MSG>
    for FakeIRStorage<ID, MSG, CPU>
{
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> FakeIRStorage<ID, MSG, CPU> {
    pub fn new(members: Vec<ID>, computer: CPU) -> Self {
        FakeIRStorage {
            records: MockRecordStore::new(),
            received_record_logs: Arc::new(RwLock::new(BTreeMap::new())),
            current_view: Arc::new(TokioRwLock::new(View {
                view: 0,
                members,
                // This is stored as normal to validate the nodes always load as recovering
                state: ViewState::Normal,
            })),
            computer_lol: computer,
        }
    }

    pub async fn set_current_view(&self, view: View<ID>) {
        let mut lock = self.current_view.write().await;
        *lock = view;
    }
}
