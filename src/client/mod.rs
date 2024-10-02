#[cfg(test)]
mod test;

use crate::io::{IRClientStorage, IRNetwork};
use crate::server::View;
use crate::types::{DecideFunction, IRMessage, NodeID};
use crate::utils::{find_quorum, Quorum, QuorumType, QuorumVote};
use crate::IRStorage;
use futures::StreamExt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cluster size is 2f+1, as per page 4 of the extended paper (3.1.2 IR Guarantees)
/// Minimum cluster size of f=1 is 3
const MINIMUM_CLUSTER_SIZE: usize = 3;

/// This is how many retries will happen in total
/// Since the network should be performing the retries, this is set to 0
/// And is only used to enforce an end cycle to the loop
const MAX_ATTEMPTS: u8 = 0;

/// The client used to interact with the IR cluster.
/// Addresses are provided via the view on the storage interface.
pub struct InconsistentReplicationClient<
    N: IRNetwork<I, M>,
    S: IRClientStorage<I, M>,
    I: NodeID,
    M: IRMessage,
> {
    network: Arc<N>,
    storage: S,
    client_id: I,
    sequence: AtomicU64,
    latest_view: View<I>,
    additional_nodes: RwLock<Vec<I>>,
    _a: PhantomData<M>,
}

struct ResponseIntermediary<I: NodeID, M: IRMessage> {
    node: I,
    message: M,
    view: View<I>,
}

impl<
        NET: IRNetwork<ID, MSG> + 'static,
        STO: IRClientStorage<ID, MSG> + 'static,
        ID: NodeID + 'static,
        MSG: IRMessage + 'static,
    > InconsistentReplicationClient<NET, STO, ID, MSG>
{
    pub async fn new(network: NET, storage: STO, client_id: ID) -> Self {
        let view = storage.recover_current_view().await;
        InconsistentReplicationClient {
            network: Arc::new(network),
            storage,
            client_id,
            sequence: AtomicU64::new(0),
            latest_view: view,
            additional_nodes: RwLock::new(Vec::with_capacity(2)),
            _a: PhantomData,
        }
    }

    /// Make an inconsistent request to the cluster
    /// Inconsistent requests happen in any order
    /// Conflict resolution is done by the client after receiving responses
    pub async fn invoke_inconsistent(&self, message: MSG) -> Result<MSG, &'static str> {
        let nodes = &self.latest_view.members;
        let nodes_len = nodes.len();

        if nodes_len < MINIMUM_CLUSTER_SIZE {
            return Err("Cluster size is too small");
        }

        // Initiate requests
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let responses = self
            .network
            .propose_inconsistent(&nodes, self.client_id.clone(), sequence, message, None)
            .await;
        let responses: Vec<_> = responses
            .into_iter()
            .filter(|(i, r)| r.is_ok())
            .map(|(i, r)| (i, r.unwrap()))
            .collect();
        let quorum: Quorum<ID, MSG> =
            find_quorum(responses.iter().map(|(node_id, (msg, view))| QuorumVote {
                node: node_id,
                message: msg,
                view,
            }))
            .map_err(|_| "Quorum not found")?;
        self.network
            .async_finalize_inconsistent(
                &quorum.view.members,
                self.client_id.clone(),
                sequence,
                quorum.message.clone(),
            )
            .await;
        Ok(quorum.message.clone())
    }

    /// Make a consistent request to the cluster
    /// Consistent requests happen in any order
    /// A provided function helps resolve conflicts once detected
    /// This same function is used during recovery
    pub async fn invoke_consistent<F: DecideFunction<MSG>>(
        &self,
        message: MSG,
        decide_function: F,
    ) -> Result<(), &'static str> {
        let current_view = self.storage.recover_current_view().await;
        let nodes = current_view.members;

        if nodes.len() < MINIMUM_CLUSTER_SIZE {
            return Err("Cluster size is too small");
        }

        // Initiate requests
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let client_id = self.client_id.clone();
        let responses = self
            .network
            .propose_consistent(&nodes, client_id, sequence, message)
            .await;
        let responses: Vec<_> = responses
            .into_iter()
            .filter(|(id, r)| r.is_ok())
            .map(|(id, r)| (id, r.unwrap()))
            .collect();
        let quorum = find_quorum(responses.iter().map(|(node_id, (msg, view))| QuorumVote {
            node: node_id,
            message: msg,
            view,
        }))
        .map_err(|_| "Quorum not found")?;

        match quorum.quorum_type {
            QuorumType::FastQuorum => {
                // We can do async finalize
                self.network
                    .async_finalize_consistent(
                        &quorum.view.members,
                        self.client_id.clone(),
                        sequence,
                        quorum.message.clone(),
                    )
                    .await;
            }
            QuorumType::NormalQuorum => {
                // TODO This is actually incorrect, we should always invoke decide if FastQuorum
                // cannot be obtained

                let responses = self
                    .network
                    .sync_finalize_consistent(
                        &quorum.view.members,
                        self.client_id.clone(),
                        sequence,
                        quorum.message.clone(),
                    )
                    .await;
                let responses: Vec<_> = responses
                    .into_iter()
                    .filter(|(i, r)| r.is_ok())
                    .map(|(i, r)| (i, r.unwrap()))
                    .collect();

                let _quorum =
                    find_quorum(responses.iter().map(|(node_id, (msg, view))| QuorumVote {
                        node: node_id,
                        message: msg,
                        view,
                    }))
                    .map_err(|_| "Unable to get enough confirm messages for consistent finalize")?;
            }
        }

        Ok(())
    }

    /// Use this function to make the client additionally make requests to these nodes
    /// Only nodes in the current view are considered for quorum, so this is a useful way to
    /// add nodes to the network.
    ///
    /// If the nodes respond with a normal view (i.e. caught up), a view change will be initiated.
    ///
    /// The list of additional nodes is cleared on view change.
    pub async fn add_nodes_to_probe(&self, nodes: Vec<ID>) {
        let mut additional_nodes = self.additional_nodes.write().await;
        additional_nodes.extend(nodes);
    }
}
