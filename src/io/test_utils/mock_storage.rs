use crate::server::{View, ViewState};
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::io::{IRClientStorage, StorageShared};

#[derive(Clone)]
pub struct MockIRStorage<ID: NodeID + 'static, MSG: IRMessage + 'static> {
    records: Arc<RwLock<BTreeMap<(ID, u64), (State, MSG)>>>,
    current_view: Arc<RwLock<View<ID>>>,
}

enum State {
    Tentative,
    Finalized,
}

impl<ID: NodeID + 'static, MSG: IRMessage + 'static> StorageShared<ID> for MockIRStorage<ID, MSG> {
    fn recover_current_view(&self) -> Pin<Box<dyn Future<Output =View<ID>>>> {
        let view = self.current_view.clone();
        Box::pin(async move { view.read().await.clone() })
    }
}

impl<ID: NodeID + 'static, MSG: IRMessage + 'static> IRStorage<ID, MSG> for MockIRStorage<ID, MSG> {
    fn record_tentative(
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

    fn promote_finalized_and_run(
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
}

impl <ID: NodeID, MSG: IRMessage> IRClientStorage<ID, MSG> for MockIRStorage<ID, MSG> {

}

impl<ID: NodeID + 'static, MSG: IRMessage + 'static> MockIRStorage<ID, MSG> {
    pub fn new(members: Vec<ID>) -> Self {
        MockIRStorage {
            records: Arc::new(RwLock::new(BTreeMap::new())),
            current_view: Arc::new(RwLock::new(View{ view: 0, members, state: ViewState::Normal})),
        }
    }

    pub async fn set_current_view(&self, view: View<ID>) {
        let mut lock = self.current_view.write().await;
        *lock = view;
    }
}
