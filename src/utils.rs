use crate::server::View;
use crate::types::{IRMessage, NodeID};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

/// Derive f (number of tolerable failures) from the number of nodes in the cluster
pub fn f(nodes: usize) -> Result<usize, ()> {
    if nodes < 3 {
        Err(())
    } else {
        Ok(((nodes - 1) as f32 / 2.0f32).ceil() as usize)
    }
}

/// Derive the fast quorum size from the number of nodes in the cluster
pub fn fast_quorum(nodes: usize) -> Result<usize, ()> {
    Ok(((3 * f(nodes)? + 1) as f32 / 2f32 + 1f32).floor() as usize)
}

/// Derive the normal quorum size from the number of nodes in the cluster
pub fn slow_quorum(nodes: usize) -> Result<usize, ()> {
    Ok(f(nodes)? + 1)
}

pub struct QuorumVote<'a, ID: NodeID, MSG: IRMessage> {
    pub(crate) node: &'a ID,
    pub(crate) message: &'a MSG,
    pub(crate) view: &'a View<ID>,
}

impl<'a, ID: NodeID, MSG: IRMessage> Clone for QuorumVote<'a, ID, MSG> {
    fn clone(&self) -> Self {
        QuorumVote {
            node: self.node,
            message: self.message,
            view: self.view,
        }
    }
}

#[derive(Eq, PartialEq)]
/// A quorum was achieved and the details are included in this struct
pub(crate) struct Quorum<'a, ID: NodeID, MSG: IRMessage> {
    pub(crate) count: usize,
    pub(crate) message: &'a MSG,
    pub(crate) nodes_with: Vec<&'a ID>,
    pub(crate) nodes_without: Vec<&'a ID>,
    pub(crate) view: &'a View<ID>,
    pub(crate) quorum_type: QuorumType,
}

impl<'a, ID: NodeID, MSG: IRMessage> Debug for Quorum<'a, ID, MSG>
where
    ID: Debug,
    MSG: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Quorum")
            .field("count", &self.count)
            .field("message", &self.message)
            .field("nodes_with", &self.nodes_with)
            .field("nodes_without", &self.nodes_without)
            .field("view", &self.view)
            .field("quorum_type", &self.quorum_type)
            .finish() // Concludes the formatting
    }
}

#[derive(Eq, PartialEq)]
/// Represents no quorum for the provided nodes, but gives enough information
/// to provide to the decide function to resolve conflicts
pub(crate) struct NoQuorum<'a, ID: NodeID, MSG: IRMessage> {
    pub(crate) view: &'a View<ID>,
    pub(crate) votes: BTreeMap<&'a MSG, Vec<&'a ID>>,
}

impl<'a, ID: NodeID, MSG: IRMessage> Debug for NoQuorum<'a, ID, MSG>
where
    ID: Debug,
    MSG: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoQuorum")
            .field("view", &self.view)
            .field("votes", &self.votes)
            .finish() // Concludes the formatting
    }
}

/// Find a quorum where the set agrees to a value
/// All views must match for quorum
/// Quorum can only be from largest view; Membership comes from largest quorum.
/// Err if no quorum
/// TODO change to return borrows not clones
pub fn find_quorum<
    'a,
    ID: NodeID,
    MSG: IRMessage,
    ITER: Iterator<Item = QuorumVote<'a, ID, MSG>>,
>(
    iterable: ITER,
) -> Result<Quorum<'a, ID, MSG>, Option<NoQuorum<'a, ID, MSG>>> {
    let mut votes: BTreeMap<&View<ID>, BTreeMap<&MSG, BTreeSet<&ID>>> = BTreeMap::new();
    let mut highest_view: Option<&'a View<ID>> = None;
    let mut all_nodes = BTreeSet::new();
    // Tally up all the votes
    for item in iterable {
        let view_entry = votes.entry(item.view).or_insert(BTreeMap::new());
        if highest_view.is_none() || item.view.view > highest_view.unwrap().view {
            highest_view = Some(item.view);
        }
        let message_entry = view_entry.entry(item.message).or_insert(BTreeSet::new());
        if !all_nodes.contains(item.node) || Some(item.view) == highest_view {
            // We don't want a node voting twice, but we also don't want to fail check
            // So we only count the second vote if it is potentially valid
            all_nodes.insert(item.node);
            message_entry.insert(item.node);
        }
    }
    let mut opposing_nodes = all_nodes;
    // Find the highest view
    let highest_view = highest_view.ok_or(None)?;
    // Find the highest number of votes
    let (quorum_vote_message, quorum_vote_nodes) = votes
        .get(highest_view)
        .ok_or(None)?
        .iter()
        .max_by(|a, b| a.1.len().cmp(&b.1.len()))
        .ok_or(None)?;
    // Handle pathological situations where there are multiple quorums
    let many_quorums: Vec<_> = votes
        .get(highest_view)
        .ok_or(None)?
        .iter()
        .filter(|(_msg, votes)| votes.len() >= quorum_vote_nodes.len())
        .collect();
    if many_quorums.len() > 1 {
        let mut votes = BTreeMap::new();
        for (msg, voters) in many_quorums {
            votes.insert(*msg, voters.iter().map(|a| *a).collect());
        }
        return Err(Some(NoQuorum {
            view: highest_view,
            votes,
        }));
    }
    // Add all nodes from the view
    for node in highest_view.members.iter() {
        opposing_nodes.insert(node);
    }
    // Remove the quorum vote nodes from the opposing nodes
    for node in quorum_vote_nodes {
        opposing_nodes.remove(node);
    }
    // Check quorum against view
    if quorum_vote_nodes.len() >= fast_quorum(highest_view.members.len()).unwrap() {
        Ok(Quorum {
            count: quorum_vote_nodes.len(),
            message: quorum_vote_message,
            nodes_with: quorum_vote_nodes.into_iter().map(|a| *a).collect(),
            nodes_without: opposing_nodes.into_iter().collect(),
            view: highest_view,
            quorum_type: QuorumType::FastQuorum,
        })
    } else if quorum_vote_nodes.len() >= slow_quorum(highest_view.members.len()).unwrap() {
        Ok(Quorum {
            count: quorum_vote_nodes.len(),
            message: quorum_vote_message,
            nodes_with: quorum_vote_nodes.into_iter().map(|a| *a).collect(),
            nodes_without: opposing_nodes.into_iter().collect(),
            view: highest_view,
            quorum_type: QuorumType::NormalQuorum,
        })
    } else {
        return Err(Some(NoQuorum {
            view: highest_view,
            votes: votes
                .get(highest_view)
                .ok_or(None)?
                .iter()
                .map(|(msg, voters)| (*msg, voters.iter().map(|a| *a).collect()))
                .collect(),
        }));
    }
}

#[derive(Eq, PartialEq)]
#[cfg(debug_assertions)]
#[derive(Debug)]
pub enum QuorumType {
    FastQuorum,
    NormalQuorum,
}

#[cfg(test)]
mod test {
    use crate::server::{View, ViewState};
    use crate::utils::{NoQuorum, Quorum, QuorumType, QuorumVote};
    use std::collections::BTreeMap;

    #[test]
    fn test_f() {
        assert!(super::f(0).is_err());
        assert!(super::f(1).is_err());
        assert!(super::f(2).is_err());
        assert_eq!(super::f(3), Ok(1));
        assert_eq!(super::f(4), Ok(2));
        assert_eq!(super::f(5), Ok(2));
        assert_eq!(super::f(6), Ok(3));
        assert_eq!(super::f(7), Ok(3));
    }

    #[test]
    fn test_fast_quorum() {
        assert!(super::fast_quorum(0).is_err());
        assert!(super::fast_quorum(1).is_err());
        assert!(super::fast_quorum(2).is_err());
        // f = 1; 3f/2+1 = 3/2+1 = 1.5+1 = 2.5 = 3
        assert_eq!(super::fast_quorum(3), Ok(3));
        // f = 2; 3f/2+1 = 6/2+1 = 3+1 = 4
        assert_eq!(super::fast_quorum(4), Ok(4));
        // f = 2; 3f/2+1 = 6/2+1 = 3+1 = 4
        assert_eq!(super::fast_quorum(5), Ok(4));
        // f = 3; 3f/2+1 = 9/2+1 = 4.5+1 = 5.5 = 6
        assert_eq!(super::fast_quorum(6), Ok(6));
        // f = 3; 3f/2+1 = 9/2+1 = 4.5+1 = 5.5 = 6
        assert_eq!(super::fast_quorum(7), Ok(6));
        // f = 4; 3f/2+1 = 12/2+1 = 6+1 = 7
        assert_eq!(super::fast_quorum(8), Ok(7));
        // f = 4; 3f/2+1 = 12/2+1 = 6+1 = 7
        assert_eq!(super::fast_quorum(9), Ok(7));
    }

    #[test]
    fn test_slow_quorum() {
        assert!(super::slow_quorum(0).is_err());
        assert!(super::slow_quorum(1).is_err());
        assert!(super::slow_quorum(2).is_err());
        assert_eq!(super::slow_quorum(3), Ok(2));
        assert_eq!(super::slow_quorum(4), Ok(3));
        assert_eq!(super::slow_quorum(5), Ok(3));
        assert_eq!(super::slow_quorum(6), Ok(4));
        assert_eq!(super::slow_quorum(7), Ok(4));
        assert_eq!(super::slow_quorum(8), Ok(5));
        assert_eq!(super::slow_quorum(9), Ok(5));
    }

    #[test]
    fn test_quorum() {
        struct TestCase<'a> {
            name: &'a str,
            line_number: u32,
            votes: Vec<QuorumVote<'a, String, String>>,
            quorum_type: QuorumType,
            expected: Result<Quorum<'a, String, String>, Option<NoQuorum<'a, String, String>>>,
        }

        fn view(num: u64, members: &[&'static str], state: ViewState) -> super::View<String> {
            super::View {
                view: num,
                members: members.iter().map(|s| s.to_string()).collect(),
                state,
            }
        }

        // Because of lifetimes we need to do some horrific shit
        let one = "1".to_string();
        let two = "2".to_string();
        let three = "3".to_string();
        let four = "4".to_string();

        let msg_a = "A".to_string();
        let msg_b = "B".to_string();

        // view_<number_of_nodes>_<view_number>_state
        let view_3_1_normal = View {
            view: 1,
            members: vec![one.clone(), two.clone(), three.clone()],
            state: ViewState::Normal,
        };
        let view_3_2_normal = view(2, &["1", "2", "3"], ViewState::Normal);
        let view_4_1_normal = view(1, &["1", "2", "3", "4"], ViewState::Normal);

        let cases: Vec<TestCase> = vec![
            // TODO quorum not achieved if no votes
            TestCase {
                name: "Quorum is achieved if all votes are the same",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 3,
                    message: &msg_a,
                    nodes_with: vec![&one, &two, &three],
                    nodes_without: vec![],
                    view: &view_3_1_normal,
                    quorum_type: QuorumType::FastQuorum,
                }),
            },
            TestCase {
                name: "Quorum is achieved if one value is different",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_b,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 2,
                    message: &msg_a,
                    nodes_with: vec![&two, &three],
                    nodes_without: vec![&one],
                    view: &view_3_1_normal,
                    quorum_type: QuorumType::NormalQuorum,
                }),
            },
            TestCase {
                name: "Quorum is achieved if one value is missing",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 2,
                    message: &msg_a,
                    nodes_with: vec![&one, &two],
                    nodes_without: vec![&three],
                    view: &view_3_1_normal,
                    quorum_type: QuorumType::NormalQuorum,
                }),
            },
            TestCase {
                name: "Quorum is not achieved is one value has a larger view",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_2_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Err(Some(NoQuorum {
                    view: &view_3_2_normal,
                    votes: BTreeMap::from([(&msg_a, vec![&three])]),
                })),
            },
            TestCase {
                name: "Quorum is achieved if one value has a smaller view",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_2_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_2_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Ok(Quorum {
                    count: 2,
                    message: &msg_a,
                    nodes_with: vec![&one, &two],
                    nodes_without: vec![&three],
                    view: &view_3_2_normal,
                    quorum_type: QuorumType::NormalQuorum,
                }),
            },
            TestCase {
                name: "Quorum is not achieved if equal split votes in 4 node cluster",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_4_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_4_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_b,
                        view: &view_4_1_normal,
                    },
                    QuorumVote {
                        node: &four,
                        message: &msg_b,
                        view: &view_4_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Err(Some(NoQuorum {
                    view: &view_4_1_normal,
                    votes: BTreeMap::from([
                        (&msg_a, vec![&one, &two]),
                        (&msg_b, vec![&three, &four]),
                    ]),
                })),
            },
            TestCase {
                name: "Double votes do not count",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Err(Some(NoQuorum {
                    view: &view_3_1_normal,
                    votes: BTreeMap::from([(&msg_a, vec![&one])]),
                })),
            },
            TestCase {
                name: "Byzantine - Node votes twice with different results",
                line_number: line!(),
                votes: vec![
                    QuorumVote {
                        node: &one,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &one,
                        message: &msg_b,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &two,
                        message: &msg_a,
                        view: &view_3_1_normal,
                    },
                    QuorumVote {
                        node: &three,
                        message: &msg_b,
                        view: &view_3_1_normal,
                    },
                ],
                quorum_type: QuorumType::NormalQuorum,
                expected: Err(Some(NoQuorum {
                    view: &view_3_1_normal,
                    votes: BTreeMap::from([
                        (&msg_a, vec![&one, &two]),
                        (&msg_b, vec![&one, &three]),
                    ]),
                })),
            },
        ];

        for case in cases {
            let result = super::find_quorum(case.votes.iter().cloned());
            assert_eq!(
                result, case.expected,
                "{} - {}",
                case.line_number, case.name
            );
        }
    }
}
