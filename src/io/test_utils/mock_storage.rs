use crate::io::{IRClientStorage, StorageShared};
use crate::server::{View, ViewState};
use crate::test_utils::mock_record_store::{MockRecordStore, OperationType, State};
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

pub trait MockOperationHandler<M: IRMessage>: Clone + 'static {
    fn exec_inconsistent(&self, message: M) -> M;
    fn exec_consistent(&self, message: M) -> M;
    fn reconcile_consistent(&self, previous: M, message: M) -> M;
}

#[derive(Clone)]
pub struct MockIRStorage<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> {
    records: MockRecordStore<ID, MSG>,
    current_view: Arc<TokioRwLock<View<ID>>>,
    computer_lol: CPU,
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> StorageShared<ID>
    for MockIRStorage<ID, MSG, CPU>
{
    fn recover_current_view(&self) -> Pin<Box<dyn Future<Output = View<ID>>>> {
        let view = self.current_view.clone();
        Box::pin(async move { view.read().await.clone() })
    }
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> IRStorage<ID, MSG>
    for MockIRStorage<ID, MSG, CPU>
{
    fn record_tentative_inconsistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
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
            computer_lol.exec_inconsistent(message)
        })
    }

    fn promote_finalized_and_exec_inconsistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
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
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        let records = self.records.clone();
        let computer = self.computer_lol.clone();
        Box::pin(async move {
            let existing = records.find_entry(client.clone(), operation).await;
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
            records
                .propose_tentative_consistent(client, operation, view, message.clone())
                .await;
            computer.exec_consistent(message)
        })
    }

    fn promote_finalized_and_reconcile_consistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        let records = self.records.clone();
        let computer = self.computer_lol.clone();
        Box::pin(async move {
            let existing = records.find_entry(client.clone(), operation).await;
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
                .promote_finalized_consistent(client, operation, view, message.clone())
                .await;
            computer.reconcile_consistent(previous, message)
        })
    }
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> IRClientStorage<ID, MSG>
    for MockIRStorage<ID, MSG, CPU>
{
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> MockIRStorage<ID, MSG, CPU> {
    pub fn new(members: Vec<ID>, computer: CPU) -> Self {
        MockIRStorage {
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
