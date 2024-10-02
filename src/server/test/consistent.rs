use crate::server::{IRServerError, View, ViewState};
use crate::test_utils::mock_computers::NoopComputer;
use crate::test_utils::{FakeIRNetwork, FakeIRStorage, MockStorage, StorageMethod};
use crate::InconsistentReplicationServer;

#[tokio::test]
pub async fn propose_rejected_if_recovering() {
    let server = InconsistentReplicationServer::new(
        FakeIRNetwork::<String, String, FakeIRStorage<_, _, NoopComputer<_>>>::new(),
        FakeIRStorage::new(
            vec!["1".to_string(), "2".to_string(), "3".to_string()],
            NoopComputer::new(),
        ),
        "1".to_string(),
    )
    .await;

    let resp = server
        .propose_consistent("client-id".to_string(), 1, "message".to_string(), None)
        .await;

    match resp {
        Ok(_) => panic!("Should fail"),
        Err(e) => match e {
            IRServerError::Recovering(r) => assert_eq!(
                r,
                View {
                    view: 0,
                    members: vec!["1".to_string(), "2".to_string(), "3".to_string()],
                    state: ViewState::Recovery,
                }
            ),
            _ => panic!("Unexpected error"),
        },
    }
}

#[tokio::test]
pub async fn finalise_rejected_if_recovering() {
    todo!()
}

#[tokio::test]
pub async fn propose_rejected_if_changing_view() {
    todo!()
}

#[tokio::test]
pub async fn finalise_rejected_if_changing_view() {
    todo!()
}

#[tokio::test]
pub async fn propose_changes_view_if_receives_higher() {
    todo!()
}

#[tokio::test]
pub async fn finalise_changes_view_if_receives_higher() {
    todo!()
}

#[tokio::test]
pub async fn propose_consistent() {
    let network = FakeIRNetwork::<String, String, MockStorage<_, _>>::new();
    let members: Vec<String> = vec!["1", "2", "3"].iter().map(|x| x.to_string()).collect();
    let view = View {
        view: 1,
        members: members.clone(),
        state: ViewState::Normal,
    };
    let storage = MockStorage::new(view.clone());
    let server =
        InconsistentReplicationServer::new(network.clone(), storage.clone(), "1".to_string()).await;

    // and
    storage.mock_record_tentative_consistent(Box::new(
        |client, seq, view, msg| -> Option<String> { Some(msg) },
    ));

    // when
    let val = server
        .propose_consistent("client-id".to_string(), 3, String::from("msg"), None)
        .await;

    // then only necessary calls
    assert!(storage.get_invocations_current_view() == vec![view.clone()]);
    assert!(
        storage.get_invocations_record_tentative_consistent()
            == vec![("client-id".to_string(), 3, view, "msg".to_string())]
    );

    // and no other calls
    storage.assert_invocations_no_calls(&[
        StorageMethod::ProposeInconsistent,
        StorageMethod::FinalizeConsistent,
        StorageMethod::FinalizeInconsistent,
    ])
}

#[tokio::test]
pub async fn propose_already_finalised() {
    todo!()
}

#[tokio::test]
pub async fn finalize_already_finalised() {
    todo!()
}

#[tokio::test]
pub async fn finalize_not_proposed() {
    todo!()
}
