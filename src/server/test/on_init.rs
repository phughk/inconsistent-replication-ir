use crate::server::{View, ViewState};
use crate::test_utils::mock_computers::NoopComputer;
use crate::test_utils::{FakeIRNetwork, FakeIRStorage};
use crate::InconsistentReplicationServer;

#[tokio::test]
pub async fn recovers_view_from_storage_and_goes_into_recovery() {
    // when
    let network = FakeIRNetwork::<String, String, FakeIRStorage<_, _, _>>::new();
    let members = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    let storage = FakeIRStorage::new(members.clone(), NoopComputer::new());
    storage
        .set_current_view(View {
            view: 3,
            members: members.clone(),
            state: ViewState::Normal,
        })
        .await;

    let server =
        InconsistentReplicationServer::new(network.clone(), storage, "1".to_string()).await;
    network.register_node("1".to_string(), server.clone());
    let lock = server.view.read().await;
    assert_eq!(
        &*lock,
        &View {
            view: 3,
            members: members.clone(),
            state: ViewState::Recovery,
        }
    );
}

#[tokio::test]
pub async fn performs_sync_with_current_view() {
    // Basically operations fail and the server sends catchup requests to it's presumed leader
    todo!()
}

#[tokio::test]
pub async fn changes_view_if_newer_view_while_recovering() {
    // Basically if the view is higher, it catches up the previous views until it synchronises all the views
    todo!()
}

#[tokio::test]
pub async fn propose_view_change_if_caught_up_and_not_member() {
    // If the node has completed catch up, then it should propose itself as a member in a new view
    todo!()
}

#[tokio::test]
pub async fn stays_in_recovery_if_no_nodes_are_reachable() {
    // Basically, we should wait until we get a message from a client about the current view
    todo!()
}
