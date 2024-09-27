use crate::types::IRMessage;

pub trait MockOperationHandler<M: IRMessage>: Clone + 'static {
    fn evaluate_inconsistent(&self, message: M) -> M;
    fn exec_inconsistent(&self, message: M) -> M;
    fn exec_consistent(&self, message: M) -> M;
    fn reconcile_consistent(&self, previous: Option<M>, message: M) -> M;
}

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
    fn evaluate_inconsistent(&self, message: M) -> M {
        message
    }
    fn exec_inconsistent(&self, message: M) -> M {
        message
    }

    fn exec_consistent(&self, message: M) -> M {
        message
    }

    fn reconcile_consistent(&self, _previous: Option<M>, message: M) -> M {
        message
    }
}
