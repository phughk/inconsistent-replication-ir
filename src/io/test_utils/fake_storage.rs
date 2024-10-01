use crate::debug::MaybeDebug;
use crate::io::{IRClientStorage, StorageShared};
use crate::server::{View, ViewState};
use crate::test_utils::mock_computers::MockOperationHandler;
use crate::test_utils::mock_record_store::{MockRecordStore, OperationType, State};
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

#[derive(Clone)]
pub struct FakeIRStorage<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> {
    records: MockRecordStore<ID, MSG>,
    current_view: Arc<TokioRwLock<View<ID>>>,
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
                    assert!(state.message == message);
                    assert!(state.operation_type == OperationType::Inconsistent);
                    assert!(state.state == State::Tentative);
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
                    assert!(state.view == view);
                    // We do not assert message, as it may be different
                    assert!(state.operation_type == OperationType::Inconsistent);
                    // Maybe this is wrong, because we may receive duplicate messages
                    assert!(state.state != State::Finalized);
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
                    assert!(state.operation_type == OperationType::Consistent);
                    assert!(state.state != State::Finalized);
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
                    assert!(state.operation_type == OperationType::Consistent);
                    assert!(state.state != State::Finalized)
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
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> IRClientStorage<ID, MSG>
    for FakeIRStorage<ID, MSG, CPU>
{
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> FakeIRStorage<ID, MSG, CPU> {
    pub fn new(members: Vec<ID>, computer: CPU) -> Self {
        FakeIRStorage {
            records: MockRecordStore::new(),
            current_view: Arc::new(TokioRwLock::new(View {
                view: 0,
                members,
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
