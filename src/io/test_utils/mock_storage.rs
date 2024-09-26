use crate::io::{IRClientStorage, StorageShared};
use crate::server::{View, ViewState};
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

pub trait MockOperationHandler<M: IRMessage>: Clone + 'static {
    fn exec_inconsistent(&self, message: M) -> M;
    fn exec_consistent(&self, message: M) -> M;
    fn reconcile_consistent(&self, previous: M, message: M) -> M;
}

#[derive(Clone)]
pub struct MockIRStorage<ID: NodeID, MSG: IRMessage, CPU: MockOperationHandler<MSG>> {
    records: Arc<RwLock<BTreeMap<(ID, u64), (State, MSG)>>>,
    current_view: Arc<RwLock<View<ID>>>,
    computer_lol: CPU,
}

enum State {
    Tentative,
    Finalized,
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
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG>>> {
        let records = self.records.clone();
        Box::pin(async move {
            let mut map = records.write().await;
            let client_clone = client.clone();
            match map.get(&(client, operation)) {
                None => {
                    map.insert(
                        (client_clone, operation),
                        (State::Tentative, message.clone()),
                    );
                    message
                }
                Some((_state, msg)) => msg.clone(),
            }
        })
    }

    fn promote_finalized_and_exec_inconsistent(
        &self,
        client: ID,
        operation: u64,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        let records = self.records.clone();
        Box::pin(async move {
            let mut map = records.write().await;
            if let Some((state, _msg)) = map.get_mut(&(client, operation)) {
                *state = State::Finalized;
            }
        })
    }

    fn record_tentative_and_exec_consistent(
        &self,
        client: ID,
        operation: u64,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG>>> {
        todo!()
    }

    fn promote_finalized_and_reconcile_consistent(
        &self,
        client: ID,
        operation: u64,
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
            records: Arc::new(RwLock::new(BTreeMap::new())),
            current_view: Arc::new(RwLock::new(View {
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
