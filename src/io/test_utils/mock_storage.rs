use crate::server::ViewState;
use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MockIRStorage<ID: NodeID + 'static, MSG: IRMessage + 'static> {
    records: Arc<RwLock<BTreeMap<(ID, u64), (State, MSG)>>>,
    current_view: Arc<RwLock<ViewState>>,
}

enum State {
    Tentative,
    Finalized,
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

    fn recover_current_view(&self) -> Pin<Box<dyn Future<Output = ViewState>>> {
        let view = self.current_view.clone();
        Box::pin(async move { view.read().await.clone() })
    }
}

impl<ID: NodeID + 'static, MSG: IRMessage + 'static> MockIRStorage<ID, MSG> {
    pub fn new() -> Self {
        MockIRStorage {
            records: Arc::new(RwLock::new(BTreeMap::new())),
            current_view: Arc::new(RwLock::new(ViewState::Normal { view: 0 })),
        }
    }

    pub async fn set_current_view(&self, view: ViewState) {
        let mut lock = self.current_view.write().await;
        *lock = view;
    }
}
