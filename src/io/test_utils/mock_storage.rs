use crate::types::{IRMessage, NodeID};
use crate::IRStorage;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MockIRStorage<ID: NodeID + 'static, MSG: IRMessage + 'static> {
    records: Arc<RwLock<BTreeMap<(ID, usize), (State, MSG)>>>,
}

enum State {
    Tentative,
    Finalized,
}

impl<ID: NodeID + 'static, MSG: IRMessage + 'static> IRStorage<ID, MSG> for MockIRStorage<ID, MSG> {
    fn record_tentative(
        &self,
        client: ID,
        operation: usize,
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

    fn promote_finalized(&self, client: ID, operation: usize) -> Pin<Box<dyn Future<Output = ()>>> {
        let records = self.records.clone();
        Box::pin(async move {
            let mut map = records.write().await;
            if let Some((state, _msg)) = map.get_mut(&(client, operation)) {
                *state = State::Finalized;
            }
        })
    }
}

impl<ID: NodeID + 'static, MSG: IRMessage + 'static> MockIRStorage<ID, MSG> {
    pub fn new() -> Self {
        MockIRStorage {
            records: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}
