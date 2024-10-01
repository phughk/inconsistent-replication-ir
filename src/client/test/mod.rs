use crate::client::InconsistentReplicationClient;
use crate::io::test_utils::{FakeIRNetwork, FakeIRStorage};
use crate::test_utils::mock_computers::NoopComputer;
use crate::types::{IRMessage, NodeID};
use crate::InconsistentReplicationServer;

#[tokio::test]
async fn client_can_make_inconsistent_requests() {
    // given a cluster
    let network = FakeIRNetwork::<_, _, FakeIRStorage<_, _, _>>::new();
    let members = vec![1, 2, 3];
    let storage = FakeIRStorage::new(members.clone(), NoopComputer::new());
    mock_cluster(&network, members).await;

    // and a client
    let client = InconsistentReplicationClient::new(network.clone(), storage, 0).await;

    // when the client makes a request
    let result = client.invoke_inconsistent(&[4, 5, 6]).await;

    // then the request is handled
    assert!(result.is_ok(), "{:?}", result);
}

#[tokio::test]
async fn client_fails_inconsistent_request_no_quorum() {
    // given a cluster
    let network = FakeIRNetwork::<_, _, FakeIRStorage<_, _, _>>::new();
    let members = vec![1, 2, 3];
    let storage = FakeIRStorage::new(members.clone(), NoopComputer::new());
    mock_cluster(&network, members).await;

    // and a client
    let client = InconsistentReplicationClient::new(network.clone(), storage, 0).await;

    // when we prevent the request from being sent
    network.drop_requests_add(1, 2000);
    network.drop_requests_add(2, 2000);

    // and we make the client perform the requests
    let result = client.invoke_inconsistent(&[4, 5, 6]).await;

    // then the request was handled
    assert!(result.is_err());
}

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
