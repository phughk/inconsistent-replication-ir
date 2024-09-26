use crate::server::View;
use crate::types::{IRMessage, NodeID};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

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
pub(crate) enum State {
    Tentative,
    Finalized,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum OperationType {
    Consistent,
    Inconsistent,
}

pub(crate) struct FullState<ID: NodeID, MSG: IRMessage> {
    pub(crate) client: ID,
    pub(crate) operation: u64,
    pub(crate) view: View<ID>,
    pub(crate) state: State,
    pub(crate) operation_type: OperationType,
    pub(crate) message: MSG,
}

#[derive(Clone)]
pub(crate) struct MockRecordStore<ID: NodeID, MSG: IRMessage> {
    records: Arc<TokioRwLock<BTreeMap<RecordKey<ID>, RecordValue<MSG>>>>,
}

impl<ID: NodeID, MSG: IRMessage> MockRecordStore<ID, MSG> {
    pub(crate) fn new() -> Self {
        MockRecordStore {
            records: Arc::new(TokioRwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) async fn find_entry(
        &self,
        client: ID,
        operation: u64,
    ) -> Option<FullState<ID, MSG>> {
        let found: Vec<FullState<ID, MSG>> = self
            .records
            .read()
            .await
            .iter()
            .filter(|(k, _v)| k.client == client && k.operation == operation)
            .map(|(k, v)| FullState {
                client: k.client.clone(),
                operation: k.operation,
                view: k.view.clone(),
                state: v.state.clone(),
                operation_type: v.operation_type.clone(),
                message: v.message.clone(),
            })
            .collect();
        assert!(found.len() <= 1);
        found.into_iter().next()
    }

    pub(crate) async fn propose_tentative_inconsistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) {
        let mut write_lock = self.records.write().await;
        write_lock.insert(
            RecordKey {
                client,
                operation,
                view,
            },
            RecordValue {
                state: State::Tentative,
                operation_type: OperationType::Inconsistent,
                message,
            },
        );
    }

    pub(crate) async fn promote_finalized_inconsistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) {
        let mut write_lock = self.records.write().await;
        write_lock.insert(
            RecordKey {
                client,
                operation,
                view,
            },
            RecordValue {
                state: State::Finalized,
                operation_type: OperationType::Inconsistent,
                message,
            },
        );
    }

    pub(crate) async fn propose_tentative_consistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) {
        let mut write_lock = self.records.write().await;
        write_lock.insert(
            RecordKey {
                client,
                operation,
                view,
            },
            RecordValue {
                state: State::Tentative,
                operation_type: OperationType::Consistent,
                message,
            },
        );
    }

    pub(crate) async fn promote_finalized_consistent(
        &self,
        client: ID,
        operation: u64,
        view: View<ID>,
        message: MSG,
    ) -> MSG {
        let mut write_lock = self.records.write().await;
        let previous = write_lock.insert(
            RecordKey {
                client,
                operation,
                view,
            },
            RecordValue {
                state: State::Finalized,
                operation_type: OperationType::Consistent,
                message: message.clone(),
            },
        );
        previous.map(|s| s.message).unwrap_or(message)
    }
}
