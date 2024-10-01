#![no_main]

mod linearizable_compute;

use crate::linearizable_compute::{LinearizableComputeOperation, LinearizableComputer};
use arbitrary::{Arbitrary, Unstructured};
use inconsistent_replication_ir::test_utils::{FakeIRNetwork, FakeIRStorage};
use inconsistent_replication_ir::types::DecideFunction;
use inconsistent_replication_ir::{InconsistentReplicationClient, InconsistentReplicationServer};
use libfuzzer_sys::{arbitrary, fuzz_target};
use std::collections::BTreeMap;

const MAX_NODES: usize = 10;
const MAX_CLIENTS: usize = 10;
const MAX_KEYS: usize = 10;

type KEY = u8;
type VALUE = u8;

#[derive(Debug)]
struct TestScenario {
    nodes: usize,
    clients: usize,
    steps: Vec<TestStep>,
}

impl<'a> Arbitrary<'a> for TestScenario {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(TestScenario {
            nodes: u.int_in_range(1..=MAX_NODES)?,
            clients: u.int_in_range(1..=MAX_CLIENTS)?,
            steps: u.arbitrary()?,
        })
    }
}

#[derive(Debug)]
enum TestStep {
    /// Make an inconsistent operation
    InconsistentMessage {
        client: usize,
        message: LinearizableComputeOperation,
    },
    /// Make a consistent operation
    ConsistentMessage {
        client: usize,
        message: LinearizableComputeOperation,
    },
    /// Make messages to the node drop
    DropRequest { who: usize },
    /// Make responses from the node drop
    DropResponse { who: usize },
    /// Turn a node on or off
    FlipSwitch { node: usize },
}

impl<'a> Arbitrary<'a> for TestStep {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let op = u.int_in_range(0..=4)?;
        match op {
            0 => Ok(TestStep::InconsistentMessage {
                client: u.int_in_range(0..=MAX_CLIENTS)?,
                message: u.arbitrary()?,
            }),
            1 => Ok(TestStep::ConsistentMessage {
                client: u.int_in_range(0..=MAX_CLIENTS)?,
                message: u.arbitrary()?,
            }),
            2 => Ok(TestStep::DropRequest {
                who: u.int_in_range(0..=MAX_NODES)?,
            }),
            3 => Ok(TestStep::DropResponse {
                who: u.int_in_range(0..=MAX_NODES)?,
            }),
            4 => Ok(TestStep::FlipSwitch {
                node: u.int_in_range(0..=MAX_NODES)?,
            }),
            _ => panic!("Unsupported arbitrary operation: {}", op),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Arbitrary)]
pub struct RequestPayload {
    reads: Vec<KEY>,
    writes: BTreeMap<KEY, VALUE>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Arbitrary)]
pub struct ResponsePayload {
    reads: BTreeMap<KEY, Vec<VALUE>>,
    writes: BTreeMap<KEY, Vec<VALUE>>,
}

pub struct TestDecideFunction {
    request: LinearizableComputeOperation,
}
impl DecideFunction<LinearizableComputeOperation> for TestDecideFunction {
    fn decide<'a, S: IntoIterator<Item = &'a LinearizableComputeOperation>>(
        &self,
        choices: S,
    ) -> &'a LinearizableComputeOperation {
        choices.into_iter().next().unwrap()
    }
}

fuzz_target!(|data: TestScenario| {
    // Create cluster
    let network = FakeIRNetwork::<_, _, FakeIRStorage<_, _, LinearizableComputer>>::new();
    let members: Vec<usize> = (0..data.nodes).collect();
    for i in 0..data.nodes {
        smol::block_on(async {
            network.register_node(
                i,
                InconsistentReplicationServer::new(
                    network.clone(),
                    FakeIRStorage::new(members.clone(), LinearizableComputer::new()),
                    i,
                )
                .await,
            );
        })
    }

    // Create clients
    let mut clients = Vec::with_capacity(data.clients);
    for client_id in 0..data.clients {
        // TODO maybe no need to clone as same thread and blocking
        let network_clone = network.clone();
        let members_clone = members.clone();
        let client = smol::block_on(async move {
            InconsistentReplicationClient::new(
                network_clone,
                // The computer isn't used on the client, but it is part of the shared storage interface
                FakeIRStorage::new(members_clone, LinearizableComputer::new()),
                client_id,
            )
            .await
        });
        clients.push(client);
    }

    // Run scenario
    for case in data.steps {
        match case {
            TestStep::InconsistentMessage { client, message } => smol::block_on(async {
                let client_id = client % clients.len();
                let _result = clients[client_id].invoke_inconsistent(message).await;
            }),
            TestStep::ConsistentMessage { client, message } => smol::block_on(async {
                let client_id = client % clients.len();
                let _result = clients[client_id]
                    .invoke_consistent(message.clone(), TestDecideFunction { request: message })
                    .await;
            }),
            TestStep::DropRequest { who } => {
                network.drop_requests_add(who, 1);
            }
            TestStep::DropResponse { who } => {
                network.drop_response_add(who, 1);
            }
            TestStep::FlipSwitch { node } => {
                let node_id = node % data.nodes;
                smol::block_on(network.switch(node_id))
            }
        }
    }
});
