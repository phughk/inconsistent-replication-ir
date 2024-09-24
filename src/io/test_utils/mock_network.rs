use crate::types::{IRMessage, NodeID};
use crate::{IRNetwork, IRStorage, InconsistentReplicationServer};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

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

    fn request_inconsistent(
        &self,
        destination: I,
        client_id: I,
        sequence: usize,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<M, ()>>>> {
        let nodes = self.nodes.clone();
        Box::pin(async move {
            let msg = nodes
                .try_read()
                .unwrap()
                .get(&destination)
                .unwrap()
                .exec_inconsistent(client_id, sequence, message)
                .await;
            Ok(msg)
        })
    }
}

impl<ID: NodeID, MSG: IRMessage, STO: IRStorage<ID, MSG>> MockIRNetwork<ID, MSG, STO> {
    pub fn new() -> Self {
        MockIRNetwork {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
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
}
