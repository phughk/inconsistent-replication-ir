use crate::test_utils::MockOperationHandler;
use crate::types::IRMessage;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use tokio::io::AsyncReadExt;

/// The operation engine that does nothing :)
#[derive(Clone)]
pub struct NoopComputer<M: IRMessage> {
    pub _phantom: std::marker::PhantomData<M>,
}

impl<M: IRMessage> NoopComputer<M> {
    pub fn new() -> Self {
        NoopComputer {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<M: IRMessage> MockOperationHandler<M> for NoopComputer<M> {
    fn exec_inconsistent(&self, message: M) -> M {
        message
    }

    fn exec_consistent(&self, message: M) -> M {
        message
    }

    fn reconcile_consistent(&self, _previous: M, message: M) -> M {
        message
    }
}

#[derive(Clone, Default)]
/// Use this for validating linearizability
/// It accepts u64 keys and the values are Vec<u64>
/// Then all operations are append
pub struct LinearizableComputer {
    data: Arc<RwLock<BTreeMap<u64, Arc<RwLock<Vec<u64>>>>>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum LinearizableComputeOperation {
    ReadRequest { key: u64 },
    ReadResponse { key: u64, value: Vec<u64> },
    WriteRequest { key: u64, value: Vec<u64> },
    WriteResponse { key: u64, value: Vec<u64> },
}

impl MockOperationHandler<LinearizableComputeOperation> for LinearizableComputer {
    fn exec_inconsistent(
        &self,
        message: LinearizableComputeOperation,
    ) -> LinearizableComputeOperation {
        self.exec(message)
    }

    fn exec_consistent(
        &self,
        message: LinearizableComputeOperation,
    ) -> LinearizableComputeOperation {
        self.exec(message)
    }

    fn reconcile_consistent(
        &self,
        previous_response: LinearizableComputeOperation,
        decided_response: LinearizableComputeOperation,
    ) -> LinearizableComputeOperation {
        match decided_response {
            LinearizableComputeOperation::ReadResponse { key, value } => {
                LinearizableComputeOperation::ReadResponse { key, value }
            }
            LinearizableComputeOperation::WriteResponse { key, value } => {
                // Linearizable requests maybe do not get resolution if there is a consistent request?
                let mut key_lock = self.data.write().unwrap();
                let previous_opt_lock_holder = key_lock
                    .insert(key, Arc::new(RwLock::new(value.clone())))
                    .clone()
                    .unwrap();
                let previous_opt_lock = previous_opt_lock_holder.read().unwrap();
                let read_previous = previous_opt_lock.clone();
                assert_eq!(
                    LinearizableComputeOperation::WriteResponse {
                        key,
                        value: read_previous
                    },
                    previous_response
                );
                LinearizableComputeOperation::WriteResponse { key, value }
            }
            _ => panic!("unsupported operation for reconcile"),
        }
    }
}

impl LinearizableComputer {
    pub fn new() -> Self {
        LinearizableComputer {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    fn exec(&self, message: LinearizableComputeOperation) -> LinearizableComputeOperation {
        match message {
            LinearizableComputeOperation::ReadRequest { key } => {
                let key_lock = self.data.read().unwrap();
                match key_lock.contains_key(&key) {
                    true => {
                        let values = key_lock.get(&key).unwrap().read().unwrap().clone();
                        LinearizableComputeOperation::ReadResponse { key, value: values }
                    }
                    false => LinearizableComputeOperation::ReadResponse {
                        key,
                        value: Vec::new(),
                    },
                }
            }
            LinearizableComputeOperation::WriteRequest { key, value } => {
                let mut key_lock = self.data.write().unwrap();
                match key_lock.contains_key(&key) {
                    true => {
                        key_lock.get(&key).unwrap().write().unwrap().extend(value);
                    }
                    false => {
                        key_lock.insert(key, Arc::new(RwLock::new(value)));
                    }
                }
                let val_lock = key_lock.get(&key).unwrap().read().unwrap();
                LinearizableComputeOperation::WriteResponse {
                    key,
                    value: val_lock.clone(),
                }
            }
            _ => panic!("unsupported operation for exec"),
        }
    }
}
