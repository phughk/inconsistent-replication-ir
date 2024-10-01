mod consistent;
mod inconsistent;
mod membership;

use crate::io::test_utils::{FakeIRNetwork, FakeIRStorage};
use crate::test_utils::mock_computers::NoopComputer;
use crate::types::{IRMessage, NodeID};
use crate::InconsistentReplicationServer;

async fn mock_cluster<ID: NodeID, MSG: IRMessage>(
    network: &FakeIRNetwork<ID, MSG, FakeIRStorage<ID, MSG, NoopComputer<MSG>>>,
    nodes: Vec<ID>,
) {
    for node_id in &nodes {
        network.register_node(
            node_id.clone(),
            InconsistentReplicationServer::new(
                network.clone(),
                FakeIRStorage::new(nodes.clone(), NoopComputer::new()),
                node_id.clone(),
            )
            .await,
        );
    }
}
