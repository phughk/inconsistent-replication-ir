use crate::server::View;
use crate::types::{IRMessage, NodeID};
use crate::{IRNetwork, IRStorage, InconsistentReplicationServer};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::RwLock as TokioRwLock;

type DropPacketCounter<ID> = Arc<StdRwLock<BTreeMap<ID, AtomicUsize>>>;

enum SwitchableNode<ID: NodeID, MSG: IRMessage, STO: IRStorage<ID, MSG>> {
    On(InconsistentReplicationServer<MockIRNetwork<ID, MSG, STO>, STO, ID, MSG>),
    Off((MockIRNetwork<ID, MSG, STO>, STO, ID)),
}

impl<ID: NodeID, MSG: IRMessage, STO: IRStorage<ID, MSG>> SwitchableNode<ID, MSG, STO> {
    async fn switch(self) -> Self {
        match self {
            SwitchableNode::On(node) => {
                let (net, sto, id, _view) = node.shutdown().await;
                SwitchableNode::Off((net, sto, id))
            }
            SwitchableNode::Off((net, storage, id)) => {
                SwitchableNode::On(InconsistentReplicationServer::new(net, storage, id).await)
            }
        }
    }
}

pub struct MockIRNetwork<
    ID: NodeID + 'static,
    MSG: IRMessage + 'static,
    STO: IRStorage<ID, MSG> + 'static,
> {
    nodes: Arc<TokioRwLock<BTreeMap<ID, SwitchableNode<ID, MSG, STO>>>>,
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
            let rl = nodes.read().await;
            let node = rl.get(&destination).unwrap();
            match node {
                SwitchableNode::On(node) => {
                    if Self::should_drop(drop_requests, &destination) {
                        return Err(());
                    }
                    let msg = node
                        .propose_inconsistent(client_id, sequence, message, highest_observed_view)
                        .await;
                    if Self::should_drop(drop_responses, &destination) {
                        return Err(());
                    }
                    Ok(msg)
                }
                SwitchableNode::Off(_) => return Err(()),
            }
        })
    }

    fn async_finalize_inconsistent(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>>>> {
        let nodes = self.nodes.clone();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            let node = read_lock.get(&destination).ok_or(())?;
            match node {
                SwitchableNode::On(node) => {
                    let (_msg, _view) = node
                        .finalize_inconsistent(client_id, sequence, message)
                        .await;
                    Ok(())
                }
                SwitchableNode::Off(_) => {
                    // Noop
                    Ok(())
                }
            }
        })
    }

    fn async_finalize_consistent(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>>>> {
        let nodes = self.nodes.clone();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            let node = read_lock.get(&destination).ok_or(())?;
            match node {
                SwitchableNode::On(node) => {
                    let (_msg, _view) =
                        node.finalize_consistent(client_id, sequence, message).await;
                    Ok(())
                }
                SwitchableNode::Off(_) => Err(()),
            }
        })
    }

    fn propose_consistent(
        &self,
        destination: I,
        client_id: I,
        sequence: u64,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<(M, View<I>), ()>>>> {
        let nodes = self.nodes.clone();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            let node = read_lock.get(&destination).ok_or(())?;
            match node {
                SwitchableNode::On(node) => {
                    let (msg, view) = node.propose_consistent(client_id, sequence, message).await;
                    Ok((msg, view))
                }
                SwitchableNode::Off(_) => Err(()),
            }
        })
    }

    fn sync_finalize_consistent(
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
            nodes: Arc::new(TokioRwLock::new(BTreeMap::new())),
            drop_requests: Arc::new(StdRwLock::new(BTreeMap::new())),
            drop_responses: Arc::new(StdRwLock::new(BTreeMap::new())),
        }
    }

    /// Register a node under an address.
    /// You should be able to clone the server, providing the network and storage are cloneable
    pub fn register_node(
        &self,
        node_id: ID,
        server: InconsistentReplicationServer<MockIRNetwork<ID, MSG, STO>, STO, ID, MSG>,
    ) {
        let node = SwitchableNode::On(server);
        self.nodes.try_write().unwrap().insert(node_id, node);
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

    pub async fn switch(&self, node_id: ID) {
        let mut write_lock = self.nodes.write().await;
        let mut val = write_lock.remove(&node_id).unwrap();
        write_lock.insert(node_id, val.switch().await);
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
