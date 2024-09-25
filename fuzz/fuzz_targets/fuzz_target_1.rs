#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use inconsistent_replication_ir::test_utils::{MockIRNetwork, MockIRStorage};
use inconsistent_replication_ir::types::DecideFunction;
use inconsistent_replication_ir::{InconsistentReplicationClient, InconsistentReplicationServer};
use libfuzzer_sys::{arbitrary, fuzz_target};

#[derive(Debug)]
struct TestScenario {
    nodes: usize,
    clients: usize,
    steps: Vec<TestStep>,
}

impl<'a> Arbitrary<'a> for TestScenario {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(TestScenario {
            nodes: u.int_in_range(1..=10)?,
            clients: u.int_in_range(1..=10)?,
            steps: u.arbitrary()?,
        })
    }
}

#[derive(arbitrary::Arbitrary, Debug)]
enum TestStep {
    InconsistentMessage { client: usize, message: u8 },
    ConsistentMessage { client: usize, message: u8 },
    DropRequest { who: usize },
    DropResponse { who: usize },
}

pub struct TestDecideFunction;
impl DecideFunction<u8> for TestDecideFunction {
    fn decide<'a, S: IntoIterator<Item = &'a u8>>(&self, _values: S) -> &'a u8 {
        _values.into_iter().next().unwrap()
    }
}

fuzz_target!(|data: TestScenario| {
    // Create cluster
    let network = MockIRNetwork::<_, _, MockIRStorage<_, _>>::new();
    for i in 0..data.nodes {
        smol::block_on(async {
            network.register_node(
                i,
                InconsistentReplicationServer::new(network.clone(), MockIRStorage::new(), i).await,
            );
        })
    }
    let mut clients = Vec::with_capacity(data.clients);
    for client_id in 0..data.clients {
        clients.push(InconsistentReplicationClient::new(
            network.clone(),
            MockIRStorage::new(),
            client_id,
        ));
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
                    .invoke_consistent(message, TestDecideFunction {})
                    .await;
            }),
            TestStep::DropRequest { who } => {
                network.drop_requests_add(who, 1);
            }
            TestStep::DropResponse { who } => {
                network.drop_response_add(who, 1);
            }
        }
    }
});
