use crate::server::View;
use crate::types::{IRMessage, NodeID};
use crate::{IRNetwork, IRStorage, InconsistentReplicationServer};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

type DropPacketCounter<ID> = Arc<RwLock<BTreeMap<ID, AtomicUsize>>>;

pub struct MockIRNetwork<
    ID: NodeID + 'static,
    MSG: IRMessage + 'static,
    STO: IRStorage<ID, MSG> + 'static,
> {
    nodes: Arc<
        RwLock<
            BTreeMap<ID, InconsistentReplicationServer<MockIRNetwork<ID, MSG, STO>, STO, ID, MSG>>,
        >,
    >,
    drop_requests: DropPacketCounter<ID>,
    drop_responses: DropPacketCounter<ID>,
}

impl<ID, MSG, STO> Clone for MockIRNetwork<ID, MSG, STO>
where
    ID: NodeID,
    MSG: IRMessage,
    STO: IRStorage<ID, MSG>,
{
    fn clone(&self) -> Self {
        MockIRNetwork {
            nodes: self.nodes.clone(),
            drop_requests: self.drop_requests.clone(),
            drop_responses: self.drop_responses.clone(),
        }
    }
}

impl<I: NodeID, M: IRMessage, STO: IRStorage<I, M>> IRNetwork<I, M> for MockIRNetwork<I, M, STO> {
    fn get_members(&self) -> Pin<Box<dyn Future<Output = Vec<I>>>> {
        let nodes = self.nodes.clone();
        Box::pin(async move {
            let a: Vec<I> = nodes.try_read().unwrap().keys().cloned().collect();
            a
        })
    }

    fn propose_inconsistent(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
        highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>> {
        let nodes = self.nodes.clone();
        let drop_requests = self.drop_requests.clone();
        let drop_responses = self.drop_responses.clone();
        Box::pin(async move {
            if Self::should_drop(drop_requests, &destination) {
                return Err(());
            }
            let msg = nodes
                .try_read()
                .unwrap()
                .get(&destination)
                .unwrap()
                .propose_inconsistent(client_id, sequence, message, highest_observed_view)
                .await;
            if Self::should_drop(drop_responses, &destination) {
                return Err(());
            }
            Ok(msg)
        })
    }

    fn propose_consistent(
        &self,
        _destination: I,
        _client_id: I,
        _sequence: u64,
        _message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>> {
        todo!()
    }

    fn async_finalize(
        &self,
        _destination: I,
        _client_id: I,
        _sequence: u64,
        _message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>>>> {
        todo!()
    }

    fn sync_finalize(
        &self,
        _destination: I,
        _client_id: I,
        _sequence: u64,
        _message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>> {
        todo!()
    }

    fn invoke_view_change(
        &self,
        _destination: I,
        _view: View<I>,
    ) -> Pin<Box<dyn Future<Output = Result<View<I>, ()>>>> {
        todo!()
    }
}

impl<ID: NodeID, MSG: IRMessage, STO: IRStorage<ID, MSG>> MockIRNetwork<ID, MSG, STO> {
    pub fn new() -> Self {
        MockIRNetwork {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            drop_requests: Arc::new(RwLock::new(BTreeMap::new())),
            drop_responses: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Register a node under an address.
    /// You should be able to clone the server, providing the network and storage are cloneable
    pub fn register_node(
        &self,
        node_id: ID,
        server: InconsistentReplicationServer<MockIRNetwork<ID, MSG, STO>, STO, ID, MSG>,
    ) {
        self.nodes.try_write().unwrap().insert(node_id, server);
    }

    pub fn drop_requests_add(&self, node_id: ID, drop_packets: usize) {
        self.drop_requests
            .try_write()
            .unwrap()
            .entry(node_id)
            .or_insert(AtomicUsize::new(0))
            .fetch_add(drop_packets, Ordering::SeqCst);
    }

    pub fn drop_response_add(&self, node_id: ID, drop_packets: usize) {
        self.drop_responses
            .try_write()
            .unwrap()
            .entry(node_id)
            .or_insert(AtomicUsize::new(0))
            .fetch_add(drop_packets, Ordering::SeqCst);
    }

    /// True, if the packet should be dropped
    fn should_drop(counter: DropPacketCounter<ID>, id: &ID) -> bool {
        let locked = counter.read().unwrap();
        match locked.get(id) {
            None => false,
            Some(c) => {
                match c.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |mut x| {
                    if x > 0 {
                        x -= 1;
                        Some(x)
                    } else {
                        None
                    }
                }) {
                    Ok(0) => false,
                    Ok(_) => true,
                    Err(_previous_value_was_zero) => false,
                }
            }
        }
    }
}
