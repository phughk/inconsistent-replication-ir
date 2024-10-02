use crate::io::IRNetworkError;
use crate::server::View;
use crate::types::{IRMessage, NodeID, OperationSequence};
use crate::{IRNetwork, IRStorage, InconsistentReplicationServer};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::RwLock as TokioRwLock;

type DropPacketCounter<ID> = Arc<StdRwLock<BTreeMap<ID, AtomicUsize>>>;

enum SwitchableNode<ID: NodeID, MSG: IRMessage, STO: IRStorage<ID, MSG>> {
    On(InconsistentReplicationServer<FakeIRNetwork<ID, MSG, STO>, STO, ID, MSG>),
    Off((FakeIRNetwork<ID, MSG, STO>, STO, ID)),
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

pub struct FakeIRNetwork<
    ID: NodeID + 'static,
    MSG: IRMessage + 'static,
    STO: IRStorage<ID, MSG> + 'static,
> {
    nodes: Arc<TokioRwLock<BTreeMap<ID, SwitchableNode<ID, MSG, STO>>>>,
    drop_requests: DropPacketCounter<ID>,
    drop_responses: DropPacketCounter<ID>,
}

impl<ID, MSG, STO> Clone for FakeIRNetwork<ID, MSG, STO>
where
    ID: NodeID,
    MSG: IRMessage,
    STO: IRStorage<ID, MSG>,
{
    fn clone(&self) -> Self {
        FakeIRNetwork {
            nodes: self.nodes.clone(),
            drop_requests: self.drop_requests.clone(),
            drop_responses: self.drop_responses.clone(),
        }
    }
}

impl<I: NodeID, M: IRMessage, STO: IRStorage<I, M>> IRNetwork<I, M> for FakeIRNetwork<I, M, STO> {
    fn propose_inconsistent(
        &self,
        destinations: &[I],
        client_id: I,
        sequence: OperationSequence,
        message: M,
        highest_observed_view: Option<View<I>>,
    ) -> Pin<Box<dyn Future<Output = Vec<(I, Result<(M, View<I>), IRNetworkError<I>>)>>>> {
        let nodes = self.nodes.clone();
        let drop_requests = self.drop_requests.clone();
        let drop_responses = self.drop_responses.clone();
        let destinations: Vec<I> = destinations.iter().cloned().collect();
        Box::pin(async move {
            let rl = nodes.read().await;
            let mut responses = Vec::with_capacity(destinations.len());
            for destination in &destinations {
                let node = rl.get(destination).unwrap();
                match node {
                    SwitchableNode::On(node) => {
                        if Self::should_drop(drop_requests.clone(), &destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        let msg = node
                            .propose_inconsistent(
                                client_id.clone(),
                                sequence,
                                message.clone(),
                                highest_observed_view.clone(),
                            )
                            .await;
                        if Self::should_drop(drop_responses.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        responses.push((destination.clone(), msg.map_err(|e| e.into())));
                    }
                    SwitchableNode::Off(_) => {
                        responses.push((
                            destination.clone(),
                            Err(IRNetworkError::NodeUnreachable(destination.clone())),
                        ));
                    }
                }
            }
            responses
        })
    }

    fn propose_consistent(
        &self,
        destinations: &[I],
        client_id: I,
        sequence: OperationSequence,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Vec<(I, Result<(M, View<I>), IRNetworkError<I>>)>>>> {
        let nodes = self.nodes.clone();
        let drop_requests = self.drop_requests.clone();
        let drop_responses = self.drop_responses.clone();
        let destinations: Vec<I> = destinations.iter().cloned().collect();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            let mut responses = Vec::with_capacity(destinations.len());
            for destination in &destinations {
                let node = read_lock
                    .get(&destination)
                    .ok_or(IRNetworkError::NodeUnreachable(destination.clone()));
                match node {
                    Ok(SwitchableNode::On(node)) => {
                        if Self::should_drop(drop_requests.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        let resp = node
                            .propose_consistent(client_id.clone(), sequence, message.clone())
                            .await;
                        if Self::should_drop(drop_responses.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        responses.push((destination.clone(), resp.map_err(|e| e.into())));
                    }
                    Ok(SwitchableNode::Off(_)) => responses.push((
                        destination.clone(),
                        Err(IRNetworkError::NodeUnreachable(destination.clone())),
                    )),
                    Err(e) => responses.push((destination.clone(), Err(e))),
                }
            }
            responses
        })
    }

    fn async_finalize_inconsistent(
        &self,
        destinations: &[I],
        client_id: I,
        sequence: OperationSequence,
        message: M,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        let nodes = self.nodes.clone();
        let drop_requests = self.drop_requests.clone();
        let drop_responses = self.drop_responses.clone();
        let destinations: Vec<I> = destinations.iter().cloned().collect();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            // TODO unnecessary, because function is async
            let mut responses = Vec::with_capacity(destinations.len());
            for destination in &destinations {
                let node = read_lock
                    .get(&destination)
                    .ok_or(IRNetworkError::NodeUnreachable(destination.clone()));
                match node {
                    Ok(SwitchableNode::On(node)) => {
                        if Self::should_drop(drop_requests.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        let resp = node
                            .finalize_inconsistent(client_id.clone(), sequence, message.clone())
                            .await;
                        if Self::should_drop(drop_responses.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        responses.push((destination.clone(), resp.map_err(|e| e.into())));
                    }
                    Ok(SwitchableNode::Off(_)) => {
                        // Noop
                        responses.push((
                            destination.clone(),
                            Err(IRNetworkError::NodeUnreachable(destination.clone())),
                        ));
                    }
                    Err(e) => {
                        responses.push((destination.clone(), Err(e)));
                    }
                }
            }
        })
    }

    fn async_finalize_consistent(
        &self,
        destinations: &[I],
        client_id: I,
        sequence: OperationSequence,
        message: M,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        let nodes = self.nodes.clone();
        let drop_requests = self.drop_requests.clone();
        let drop_responses = self.drop_responses.clone();
        let destinations: Vec<I> = destinations.iter().cloned().collect();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            for destination in destinations {
                let node = read_lock
                    .get(&destination)
                    .ok_or(IRNetworkError::NodeUnreachable(destination.clone()));
                match node {
                    Ok(SwitchableNode::On(node)) => {
                        if Self::should_drop(drop_requests.clone(), &destination) {
                            continue;
                        }
                        let resp = node
                            .finalize_consistent(client_id.clone(), sequence, message.clone())
                            .await;
                        if Self::should_drop(drop_responses.clone(), &destination) {
                            continue;
                        }
                    }
                    Ok(SwitchableNode::Off(_)) => continue,
                    Err(_) => continue,
                }
            }
        })
    }

    fn sync_finalize_consistent(
        &self,
        destinations: &[I],
        client_id: I,
        sequence: OperationSequence,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Vec<(I, Result<(M, View<I>), IRNetworkError<I>>)>>>> {
        let nodes = self.nodes.clone();
        let drop_requests = self.drop_requests.clone();
        let drop_responses = self.drop_responses.clone();
        let destinations: Vec<I> = destinations.iter().cloned().collect();
        Box::pin(async move {
            let read_lock = nodes.read().await;
            let mut responses = Vec::with_capacity(destinations.len());
            for destination in &destinations {
                let node = read_lock
                    .get(&destination)
                    .ok_or(IRNetworkError::NodeUnreachable(destination.clone()));
                match node {
                    Ok(SwitchableNode::On(node)) => {
                        if Self::should_drop(drop_requests.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        let resp = node
                            .finalize_consistent(client_id.clone(), sequence, message.clone())
                            .await;
                        if Self::should_drop(drop_responses.clone(), destination) {
                            responses.push((
                                destination.clone(),
                                Err(IRNetworkError::NodeUnreachable(destination.clone())),
                            ));
                            continue;
                        }
                        responses.push((destination.clone(), resp.map_err(|e| e.into())));
                    }
                    Ok(SwitchableNode::Off(_)) => responses.push((
                        destination.clone(),
                        Err(IRNetworkError::NodeUnreachable(destination.clone())),
                    )),
                    Err(e) => responses.push((destination.clone(), Err(e))),
                }
            }
            responses
        })
    }
}

impl<ID: NodeID, MSG: IRMessage, STO: IRStorage<ID, MSG>> FakeIRNetwork<ID, MSG, STO> {
    pub fn new() -> Self {
        FakeIRNetwork {
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
        server: InconsistentReplicationServer<FakeIRNetwork<ID, MSG, STO>, STO, ID, MSG>,
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

    /// Perform all the maintenance tasks of all associated nodes
    pub async fn do_all_maintenance(&self) {
        let node_lock = self.nodes.read().await;
        for node in node_lock.values() {
            if let SwitchableNode::On(node) = node {
                node.perform_maintenance().await;
            }
        }
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
