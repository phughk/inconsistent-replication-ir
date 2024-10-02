use crate::server::{IROperation, View};
use crate::types::{IRMessage, NodeID, OperationSequence};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct RecordKey<ID: NodeID> {
    client: ID,
    sequence: OperationSequence,
    view: View<ID>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct RecordValue<MSG: IRMessage> {
    state: State,
    operation_type: OperationType,
    operation: MSG,
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
    pub(crate) ir_operation: IROperation<ID, MSG>,
    pub(crate) view: View<ID>,
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
        operation: OperationSequence,
    ) -> Option<FullState<ID, MSG>> {
        let found: Vec<FullState<ID, MSG>> = self
            .records
            .read()
            .await
            .iter()
            .filter(|(k, _v)| k.client == client && k.sequence == operation)
            .map(|(k, v)| {
                let op = match v.operation_type {
                    OperationType::Consistent => match v.state {
                        State::Tentative => IROperation::ConsistentPropose {
                            client: k.client.clone(),
                            sequence: k.sequence,
                            message: v.operation.clone(),
                        },
                        State::Finalized => IROperation::ConsistentFinalize {
                            client: k.client.clone(),
                            sequence: k.sequence,
                            message: v.operation.clone(),
                        },
                    },
                    OperationType::Inconsistent => match v.state {
                        State::Tentative => IROperation::InconsistentPropose {
                            client: k.client.clone(),
                            sequence: k.sequence,
                            message: v.operation.clone(),
                        },
                        State::Finalized => IROperation::InconsistentFinalize {
                            client: k.client.clone(),
                            sequence: k.sequence,
                            message: v.operation.clone(),
                        },
                    },
                };
                FullState {
                    ir_operation: op,
                    view: k.view.clone(),
                }
            })
            .collect();
        assert!(found.len() <= 1);
        found.into_iter().next()
    }

    pub(crate) async fn propose_tentative_inconsistent(
        &self,
        client: ID,
        sequence: OperationSequence,
        view: View<ID>,
        operation: MSG,
    ) {
        let mut write_lock = self.records.write().await;
        write_lock.insert(
            RecordKey {
                client,
                sequence: sequence,
                view,
            },
            RecordValue {
                state: State::Tentative,
                operation_type: OperationType::Inconsistent,
                operation,
            },
        );
    }

    pub(crate) async fn promote_finalized_inconsistent(
        &self,
        client: ID,
        sequence: OperationSequence,
        view: View<ID>,
        message: MSG,
    ) {
        let mut write_lock = self.records.write().await;
        write_lock.insert(
            RecordKey {
                client,
                sequence,
                view,
            },
            RecordValue {
                state: State::Finalized,
                operation_type: OperationType::Inconsistent,
                operation: message,
            },
        );
    }

    pub(crate) async fn propose_tentative_consistent(
        &self,
        client: ID,
        sequence: OperationSequence,
        view: View<ID>,
        operation: MSG,
    ) {
        let mut write_lock = self.records.write().await;
        write_lock.insert(
            RecordKey {
                client,
                sequence,
                view,
            },
            RecordValue {
                state: State::Tentative,
                operation_type: OperationType::Consistent,
                operation,
            },
        );
    }

    pub(crate) async fn promote_finalized_consistent_returning_previous_evaluation(
        &self,
        client: ID,
        sequence: OperationSequence,
        view: View<ID>,
        operation: MSG,
    ) -> Option<MSG> {
        let mut write_lock = self.records.write().await;
        let key = RecordKey {
            client,
            sequence,
            view,
        };
        let previous = write_lock.insert(
            key,
            RecordValue {
                state: State::Finalized,
                operation_type: OperationType::Consistent,
                operation: operation.clone(),
            },
        );
        previous.map(|s| s.operation)
    }
}
