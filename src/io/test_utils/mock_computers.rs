use crate::test_utils::MockOperationHandler;
use crate::types::IRMessage;

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
