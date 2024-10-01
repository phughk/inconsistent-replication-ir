use crate::io::StorageShared;
use crate::server::View;
use crate::types::{IRMessage, NodeID, OperationSequence};
use crate::IRStorage;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
/// MockStorage tracks all the calls that happen to the storage, and provides mocked responses
/// Tbh this should be seldom used - it's much better to access state via real implementations
pub struct MockStorage<ID: NodeID, MSG: IRMessage> {
    current_view: Arc<RwLock<View<ID>>>,
    record_recover_current_view: Arc<RwLock<Vec<View<ID>>>>,

    record_tentative_inconsistent_log: Arc<RwLock<Vec<(ID, OperationSequence, View<ID>, MSG)>>>,
    matcher_record_tentative_inconsistent:
        Arc<RwLock<Vec<Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<MSG>>>>>,

    promote_finalized_inconsistent_log: Arc<RwLock<Vec<(ID, OperationSequence, View<ID>, MSG)>>>,
    matcher_promote_finalized_inconsistent:
        Arc<RwLock<Vec<Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<()>>>>>,

    record_tentative_consistent_log: Arc<RwLock<Vec<(ID, OperationSequence, View<ID>, MSG)>>>,
    matcher_record_tentative_consistent:
        Arc<RwLock<Vec<Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<MSG>>>>>,

    promote_finalized_consistent_log: Arc<RwLock<Vec<(ID, OperationSequence, View<ID>, MSG)>>>,
    matcher_promote_finalized_consistent:
        Arc<RwLock<Vec<Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<MSG>>>>>,
}

impl<ID: NodeID, MSG: IRMessage> StorageShared<ID> for MockStorage<ID, MSG> {
    fn recover_current_view(&self) -> Pin<Box<dyn Future<Output = View<ID>> + 'static>> {
        let view = self.current_view.read().unwrap().clone();
        let view_record = self
            .record_recover_current_view
            .write()
            .unwrap()
            .push(view.clone());
        Box::pin(async move { view })
    }
}

impl<ID: NodeID, MSG: IRMessage> IRStorage<ID, MSG> for MockStorage<ID, MSG> {
    fn record_tentative_inconsistent_and_evaluate(
        &self,
        client: ID,
        operation: OperationSequence,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        self.record_tentative_inconsistent_log
            .write()
            .unwrap()
            .push((client.clone(), operation, view.clone(), message.clone()));
        let matchers = self.matcher_record_tentative_inconsistent.clone();
        Box::pin(async move {
            matchers
                .read()
                .unwrap()
                .iter()
                .map(|f| f(client.clone(), operation, view.clone(), message.clone()))
                .find(|res| res.is_some())
                .flatten()
                .unwrap()
        })
    }

    fn promote_finalized_and_exec_inconsistent(
        &self,
        client: ID,
        operation: OperationSequence,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        self.promote_finalized_inconsistent_log
            .write()
            .unwrap()
            .push((client.clone(), operation, view.clone(), message.clone()));
        let matchers = self.matcher_promote_finalized_inconsistent.clone();
        Box::pin(async move {
            matchers
                .read()
                .unwrap()
                .iter()
                .map(|f| f(client.clone(), operation, view.clone(), message.clone()))
                .find(|f| f.is_some())
                .flatten()
                .ok_or("No matching mock for finalized inconsistent")
                .unwrap();
        })
    }

    fn record_tentative_and_exec_consistent(
        &self,
        client: ID,
        operation: OperationSequence,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        self.record_tentative_consistent_log.write().unwrap().push((
            client.clone(),
            operation,
            view.clone(),
            message.clone(),
        ));
        let matchers = self.matcher_record_tentative_consistent.clone();
        Box::pin(async move {
            matchers
                .read()
                .unwrap()
                .iter()
                .map(|f| f(client.clone(), operation, view.clone(), message.clone()))
                .find(|f| f.is_some())
                .flatten()
                .ok_or("No matching mock for tentative consistent")
                .unwrap()
        })
    }

    fn promote_finalized_and_reconcile_consistent(
        &self,
        client: ID,
        operation: OperationSequence,
        view: View<ID>,
        message: MSG,
    ) -> Pin<Box<dyn Future<Output = MSG> + 'static>> {
        self.promote_finalized_consistent_log
            .write()
            .unwrap()
            .push((client.clone(), operation, view.clone(), message.clone()));
        let matchers = self.matcher_promote_finalized_consistent.clone();
        Box::pin(async move {
            matchers
                .read()
                .unwrap()
                .iter()
                .map(|f| f(client.clone(), operation, view.clone(), message.clone()))
                .find(|f| f.is_some())
                .flatten()
                .unwrap()
        })
    }
}

impl<ID: NodeID, MSG: IRMessage> MockStorage<ID, MSG> {
    pub fn new(current_view: View<ID>) -> MockStorage<ID, MSG> {
        MockStorage {
            current_view: Arc::new(RwLock::new(current_view)),
            record_recover_current_view: Arc::new(Default::default()),
            record_tentative_inconsistent_log: Arc::new(Default::default()),
            matcher_record_tentative_inconsistent: Arc::new(Default::default()),
            promote_finalized_inconsistent_log: Arc::new(Default::default()),
            matcher_promote_finalized_inconsistent: Arc::new(Default::default()),
            record_tentative_consistent_log: Arc::new(Default::default()),
            matcher_record_tentative_consistent: Arc::new(Default::default()),
            promote_finalized_consistent_log: Arc::new(Default::default()),
            matcher_promote_finalized_consistent: Arc::new(Default::default()),
        }
    }
    pub fn mock_record_tentative_inconsistent_and_evaluate(
        &self,
        matcher: Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<MSG>>,
    ) {
        self.matcher_record_tentative_inconsistent
            .write()
            .unwrap()
            .push(matcher);
    }

    pub fn mock_record_tentative_consistent(
        &self,
        matcher: Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<MSG>>,
    ) {
        self.matcher_record_tentative_consistent
            .write()
            .unwrap()
            .push(matcher);
    }

    pub fn mock_promote_inconsistent(
        &self,
        matcher: Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<()>>,
    ) {
        self.matcher_promote_finalized_inconsistent
            .write()
            .unwrap()
            .push(matcher);
    }

    pub fn mock_promote_consistent(
        &self,
        matcher: Box<dyn Fn(ID, OperationSequence, View<ID>, MSG) -> Option<MSG>>,
    ) {
        self.matcher_promote_finalized_consistent
            .write()
            .unwrap()
            .push(matcher);
    }

    pub fn get_invocations_current_view(&self) -> Vec<View<ID>> {
        self.record_recover_current_view.read().unwrap().clone()
    }

    pub fn get_invocations_record_tentative_consistent(
        &self,
    ) -> Vec<(ID, OperationSequence, View<ID>, MSG)> {
        self.record_tentative_consistent_log.read().unwrap().clone()
    }

    pub fn get_invocations_record_tentative_inconsistent(
        &self,
    ) -> Vec<(ID, OperationSequence, View<ID>, MSG)> {
        self.record_tentative_inconsistent_log
            .read()
            .unwrap()
            .clone()
    }

    pub fn get_invocations_promote_inconsistent(
        &self,
    ) -> Vec<(ID, OperationSequence, View<ID>, MSG)> {
        self.promote_finalized_inconsistent_log
            .read()
            .unwrap()
            .clone()
    }

    pub fn get_invocations_promote_consistent(
        &self,
    ) -> Vec<(ID, OperationSequence, View<ID>, MSG)> {
        self.promote_finalized_consistent_log
            .read()
            .unwrap()
            .clone()
    }

    pub fn assert_invocations_no_calls(&self, methods: &[StorageMethod]) {
        for method in methods {
            match method {
                StorageMethod::ProposeInconsistent => {
                    assert!(
                        self.get_invocations_record_tentative_inconsistent()
                            == Vec::with_capacity(0)
                    )
                }
                StorageMethod::ProposeConsistent => {
                    assert!(
                        self.get_invocations_record_tentative_consistent() == Vec::with_capacity(0)
                    )
                }
                StorageMethod::FinalizeInconsistent => {
                    assert!(self.get_invocations_promote_inconsistent() == Vec::with_capacity(0))
                }
                StorageMethod::FinalizeConsistent => {
                    assert!(self.get_invocations_promote_consistent() == Vec::with_capacity(0))
                }
            }
        }
    }
}

pub enum StorageMethod {
    ProposeInconsistent,
    ProposeConsistent,
    FinalizeInconsistent,
    FinalizeConsistent,
}
