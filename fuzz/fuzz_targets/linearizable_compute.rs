use arbitrary::{Arbitrary, Unstructured};
use inconsistent_replication_ir::test_utils::MockOperationHandler;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
/// Use this for validating linearizability
/// It accepts u64 keys and the values are Vec<u64>
/// Then all operations are append
pub struct LinearizableComputer {
    data: Arc<RwLock<BTreeMap<u64, Arc<RwLock<Vec<u64>>>>>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum LinearizableComputeOperation {
    ReadOperation {
        key: u64,
        computed_value: Option<Vec<u64>>,
    },
    WriteOperation {
        key: u64,
        requested_value: Vec<u64>,
        computed_value: Option<Vec<u64>>,
    },
}

impl MockOperationHandler<LinearizableComputeOperation> for LinearizableComputer {
    fn evaluate_inconsistent(
        &self,
        message: LinearizableComputeOperation,
    ) -> LinearizableComputeOperation {
        self.exec(message)
    }

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
        _previous_response: Option<LinearizableComputeOperation>,
        decided_response: LinearizableComputeOperation,
    ) -> LinearizableComputeOperation {
        match decided_response {
            LinearizableComputeOperation::ReadOperation {
                key,
                computed_value,
            } => {
                // There is nothing to reconcile for read operations
                LinearizableComputeOperation::ReadOperation {
                    key,
                    computed_value,
                }
            }
            LinearizableComputeOperation::WriteOperation {
                key,
                requested_value,
                computed_value,
            } => {
                // We are going to ignore our previous response and just apply the decided response
                LinearizableComputeOperation::WriteOperation {
                    key,
                    requested_value,
                    computed_value,
                }
            }
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
            LinearizableComputeOperation::ReadOperation {
                key,
                computed_value,
            } => {
                // Inconsistent operations execute twice - once for evaluation(non-persistent), once for finalize
                let key_lock = self.data.read().unwrap();
                match key_lock.contains_key(&key) {
                    true => {
                        let values = key_lock.get(&key).unwrap().read().unwrap().clone();
                        LinearizableComputeOperation::ReadOperation {
                            key,
                            computed_value: Some(values),
                        }
                    }
                    false => LinearizableComputeOperation::ReadOperation {
                        key,
                        computed_value: Some(Vec::with_capacity(2)),
                    },
                }
            }
            LinearizableComputeOperation::WriteOperation {
                key,
                requested_value,
                computed_value,
            } => {
                let mut key_lock = self.data.write().unwrap();
                match key_lock.contains_key(&key) {
                    true => {
                        // Noop, we have the key
                    }
                    false => {
                        key_lock.insert(key, Arc::new(RwLock::new(Vec::with_capacity(2))));
                    }
                }
                drop(key_lock);
                let key_lock = self.data.read().unwrap();
                let key_entry = key_lock.get(&key).unwrap().clone();
                let mut val_lock = key_entry.write().unwrap();
                val_lock.extend(computed_value.unwrap_or(Vec::with_capacity(2)));
                LinearizableComputeOperation::WriteOperation {
                    key,
                    requested_value,
                    computed_value: Some(val_lock.clone()),
                }
            }
        }
    }
}

impl<'a> Arbitrary<'a> for LinearizableComputeOperation {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let op_choice = u.int_in_range(0..=1)?;
        if op_choice == 0 {
            Ok(LinearizableComputeOperation::ReadOperation {
                key: u.arbitrary()?,
                computed_value: None,
            })
        } else {
            Ok(LinearizableComputeOperation::WriteOperation {
                key: u.arbitrary()?,
                requested_value: u.arbitrary()?,
                computed_value: None,
            })
        }
    }
}
