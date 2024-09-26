use crate::io::{IRClientStorage, StorageShared};
use crate::server::{View, ViewState};
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

pub trait MockOperationHandler<M: IRMessage>: Clone + 'static {
    fn exec_inconsistent(&self, message: M) -> M;
    fn exec_consistent(&self, message: M) -> M;
    fn reconcile_consistent(&self, previous: M, message: M) -> M;
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct RecordKey<ID: NodeID> {
    client: ID,
    operation: u64,
    view: View<ID>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct RecordValue<MSG: IRMessage> {
    state: State,
    operation_type: OperationType,
    message: MSG,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum State {
    Tentative,
    Finalized,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
enum OperationType {
    Consistent,
    Inconsistent,
}

#[derive(Clone)]
pub struct MockIRStorage<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> {
    records: Arc<TokioRwLock<BTreeMap<RecordKey<ID>, RecordValue<MSG>>>>,
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
    ) -> Pin<Box<dyn Future<Output = MSG>>> {
        let records = self.records.clone();
        Box::pin(async move {
            let mut map = records.write().await;
            let client_clone = client.clone();
            match map.get(&RecordKey {
                client,
                operation,
                view: view.clone(),
            }) {
                None => {
                    map.insert(
                        RecordKey {
                            client: client_clone,
                            operation,
                            view,
                        },
                        RecordValue {
                            state: State::Tentative,
                            operation_type: OperationType::Inconsistent,
                            message: message.clone(),
                        },
                    );
                    message
                }
                Some(r) => r.message.clone(),
            }
        })
    }

    fn promote_finalized_and_exec_inconsistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        let records = self.records.clone();
        Box::pin(async move {
            let mut map = records.write().await;
            if let Some(value) = map.get_mut(&RecordKey {
                client: client,
                operation,
                view: view,
            }) {
                value.state = State::Finalized;
                value.message = message;
            }
        })
    }

    fn record_tentative_and_exec_consistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG>>> {
        let records = self.records.clone();
        Box::pin(async move {
            let mut map = records.write().await;
            match map.get(&RecordKey {
                client: client.clone(),
                operation,
                view: view.clone(),
            }) {
                None => {
                    map.insert(
                        RecordKey {
                            client,
                            operation,
                            view,
                        },
                        RecordValue {
                            state: State::Tentative,
                            operation_type: OperationType::Consistent,
                            message: message.clone(),
                        },
                    );
                    message
                }
                Some(r) => r.message.clone(),
            }
        })
    }

    fn promote_finalized_and_reconcile_consistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG>>> {
        todo!()
    }
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> IRClientStorage<ID, MSG>
    for MockIRStorage<ID, MSG, CPU>
{
}

impl<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> MockIRStorage<ID, MSG, CPU> {
    pub fn new(members: Vec<ID>, computer: CPU) -> Self {
        MockIRStorage {
            records: Arc::new(TokioRwLock::new(BTreeMap::new())),
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
