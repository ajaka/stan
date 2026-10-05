use std::collections::HashMap;

use tokio::sync::mpsc::Sender;

use crate::common::utils::split;

use crate::core::{
    group::{Group, MessagePayload},
    types::WriterMessage,
};

pub struct Trie {
    nodes: Vec<Node>,
    free: Vec<usize>,
}

struct Node {
    children: HashMap<String, usize>,
    groups: HashMap<String, Group>,
}

impl Node {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.groups.is_empty()
    }
}

impl Trie {
    pub fn new() -> Self {
        Self {
            nodes: vec![Node::new()],
            free: Vec::new(),
        }
    }

    pub fn add_sub(
        &mut self,
        path: String,
        group: String,
        sender: Sender<WriterMessage>,
        sub_id: u8,
        conn_id: usize,
    ) {
        let segments = split(&path);

        let idx = self.walk_create(&segments);
        self.nodes[idx]
            .groups
            .entry(group)
            .or_default()
            .add_conn(sender, sub_id, conn_id);
    }

    fn walk_create(&mut self, segments: &[&str]) -> usize {
        let mut current = 0;

        for seg in segments {
            let next = self.nodes[current].children.get(*seg).copied();
            current = match next {
                Some(i) => i,
                None => {
                    let new_idx = match self.free.pop() {
                        Some(i) => {
                            self.nodes[i] = Node::new();
                            i
                        }
                        None => {
                            let i = self.nodes.len();
                            self.nodes.push(Node::new());
                            i
                        }
                    };
                    self.nodes[current]
                        .children
                        .insert(seg.to_string(), new_idx);
                    new_idx
                }
            };
        }

        current
    }

    pub fn send_message(&mut self, topic: String, msg: MessagePayload) -> usize {
        let segments = split(&topic);

        let mut matched = Vec::new();
        self.collect(0, &segments, 0, &mut matched);

        let mut delivered = 0;
        for idx in matched {
            for group in self.nodes[idx].groups.values_mut() {
                if group.send(&msg) {
                    delivered += 1;
                }
            }
        }
        delivered
    }

    fn collect(&self, node_idx: usize, segments: &[&str], i: usize, out: &mut Vec<usize>) {
        if i == segments.len() {
            if !self.nodes[node_idx].groups.is_empty() {
                out.push(node_idx);
            }
            return;
        }

        let node = &self.nodes[node_idx];

        // A match guard rather than an `if let &&` let-chain: those need Rust
        // 1.88, and this crate's MSRV is 1.85.
        match node.children.get(">") {
            Some(&rec) if !self.nodes[rec].groups.is_empty() => out.push(rec),
            _ => {}
        }

        if let Some(&star) = node.children.get("*") {
            self.collect(star, segments, i + 1, out);
        }

        if let Some(&child) = node.children.get(segments[i]) {
            self.collect(child, segments, i + 1, out);
        }
    }

    pub fn remove_sub(&mut self, path: String, group: String, conn_id: usize) {
        let segments = split(&path);
        self.remove_from(0, &segments, &group, conn_id);
    }

    fn remove_from(
        &mut self,
        node_idx: usize,
        segments: &[&str],
        group: &str,
        conn_id: usize,
    ) -> bool {
        if segments.is_empty() {
            if let Some(g) = self.nodes[node_idx].groups.get_mut(group) {
                g.remove_conn(conn_id);
                if g.is_empty() {
                    self.nodes[node_idx].groups.remove(group);
                }
            }
            return self.nodes[node_idx].is_empty();
        }

        let seg = segments[0];
        let Some(child) = self.nodes[node_idx].children.get(seg).copied() else {
            return false;
        };

        if self.remove_from(child, &segments[1..], group, conn_id) {
            self.nodes[node_idx].children.remove(seg);
            self.free.push(child);
        }

        self.nodes[node_idx].is_empty()
    }

    pub fn remove_conn(&mut self, conn_id: usize) {
        for node in self.nodes.iter_mut() {
            for group in node.groups.values_mut() {
                group.remove_conn(conn_id);
            }
            node.groups.retain(|_, g| !g.is_empty());
        }

        self.prune_from(0);
    }
    fn prune_from(&mut self, node_idx: usize) -> bool {
        let children: Vec<usize> = self.nodes[node_idx].children.values().copied().collect();

        for child in children {
            if self.prune_from(child) {
                let seg = self.nodes[node_idx]
                    .children
                    .iter()
                    .find(|&(_, &c)| c == child)
                    .map(|(s, _)| s.clone());
                if let Some(seg) = seg {
                    self.nodes[node_idx].children.remove(&seg);
                }
                self.free.push(child);
                self.nodes[child] = Node::new();
            }
        }

        self.nodes[node_idx].is_empty()
    }
}

impl Default for Trie {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::{self, Receiver};

    fn chan() -> (Sender<WriterMessage>, Receiver<WriterMessage>) {
        mpsc::channel(64)
    }

    fn payload() -> MessagePayload {
        MessagePayload {
            payload: b"hello".to_vec(),
            timestamp: 7,
            id: 1,
        }
    }

    /// Subscribes `topic` in `group` and returns a receiver to inspect.
    fn sub(trie: &mut Trie, topic: &str, group: &str) -> Receiver<WriterMessage> {
        let (tx, rx) = chan();
        trie.add_sub(topic.to_string(), group.to_string(), tx, 1, 1);
        rx
    }

    fn count(rx: &mut Receiver<WriterMessage>) -> usize {
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        n
    }

    // ------------------------------------------------------------ exact match

    #[test]
    fn exact_topic_reaches_only_its_own_subscriber() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.bar", "g");

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        assert_eq!(count(&mut rx), 1);
    }

    #[test]
    fn exact_topic_does_not_match_a_deeper_topic() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.bar", "g");

        assert_eq!(trie.send_message("foo.bar.baz".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn exact_topic_does_not_match_a_parent_topic() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.bar", "g");

        assert_eq!(trie.send_message("foo".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn publish_to_an_unknown_topic_delivers_nowhere() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.bar", "g");

        assert_eq!(trie.send_message("other.topic".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn shared_prefixes_reuse_one_node_per_segment() {
        let mut trie = Trie::new();
        sub(&mut trie, "foo.bar.baz", "g");
        sub(&mut trie, "foo.bar.qux", "g");

        // root + foo + bar + baz + qux
        assert_eq!(trie.nodes.len(), 5);
    }

    // ------------------------------------------------------------------- star

    #[test]
    fn star_matches_exactly_one_segment() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.*", "g");

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        assert_eq!(count(&mut rx), 1);
    }

    #[test]
    fn star_does_not_match_two_segments() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.*", "g");

        assert_eq!(trie.send_message("foo.bar.baz".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn star_does_not_match_zero_segments() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.*", "g");

        assert_eq!(trie.send_message("foo".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    // --------------------------------------------------------------------- >

    #[test]
    fn gt_matches_one_or_more_remaining_segments() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.>", "g");

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        assert_eq!(trie.send_message("foo.bar.baz".into(), payload()), 1);
        assert_eq!(count(&mut rx), 2);
    }

    #[test]
    fn gt_requires_at_least_one_segment() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.>", "g");

        assert_eq!(trie.send_message("foo".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn root_gt_matches_everything_non_empty() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, ">", "g");

        assert_eq!(trie.send_message("foo".into(), payload()), 1);
        assert_eq!(trie.send_message("a.b.c.d".into(), payload()), 1);
        assert_eq!(count(&mut rx), 2);
    }

    #[test]
    fn gt_and_literal_subscriptions_both_match_the_same_publish() {
        let mut trie = Trie::new();
        let mut gt = sub(&mut trie, ">", "g1");
        let mut literal = sub(&mut trie, "foo.bar", "g2");

        // `>` is taken whole rather than descended into, and the literal
        // subscriber is still reached independently.
        assert_eq!(trie.send_message("foo.bar".into(), payload()), 2);
        assert_eq!(count(&mut gt), 1);
        assert_eq!(count(&mut literal), 1);
    }

    // --------------------------------------------------------- combinations

    #[test]
    fn overlapping_subscriptions_each_get_a_copy() {
        let mut trie = Trie::new();
        let mut exact = sub(&mut trie, "foo.bar", "g1");
        let mut star = sub(&mut trie, "foo.*", "g2");
        let mut gt = sub(&mut trie, "foo.>", "g3");

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 3);

        assert_eq!(count(&mut exact), 1);
        assert_eq!(count(&mut star), 1);
        assert_eq!(count(&mut gt), 1);
    }

    #[test]
    fn one_group_with_two_connections_delivers_to_one_of_them() {
        let mut trie = Trie::new();
        let (tx_a, mut rx_a) = chan();
        let (tx_b, mut rx_b) = chan();
        trie.add_sub("foo.bar".into(), "workers".into(), tx_a, 1, 10);
        trie.add_sub("foo.bar".into(), "workers".into(), tx_b, 1, 20);

        for _ in 0..4 {
            assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        }

        assert_eq!(count(&mut rx_a) + count(&mut rx_b), 4);
    }

    #[test]
    fn separate_groups_receive_independently() {
        let mut trie = Trie::new();
        let mut a = sub(&mut trie, "foo.bar", "g1");
        let mut b = sub(&mut trie, "foo.bar", "g2");

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 2);
        assert_eq!(count(&mut a), 1);
        assert_eq!(count(&mut b), 1);
    }

    // --------------------------------------------------------------- removal

    #[test]
    fn unsubscribe_stops_delivery() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.bar", "g");
        trie.remove_sub("foo.bar".into(), "g".into(), 1);

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn unsubscribing_one_group_leaves_the_others() {
        let mut trie = Trie::new();
        let mut a = sub(&mut trie, "foo.bar", "g1");
        let mut b = sub(&mut trie, "foo.bar", "g2");
        trie.remove_sub("foo.bar".into(), "g1".into(), 1);

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        assert_eq!(count(&mut a), 0);
        assert_eq!(count(&mut b), 1);
    }

    #[test]
    fn unsubscribing_a_wildcard_uses_the_wildcard_path() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.*", "g");
        trie.remove_sub("foo.*".into(), "g".into(), 1);

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    #[test]
    fn unsubscribing_an_unknown_topic_is_a_no_op() {
        let mut trie = Trie::new();
        let mut rx = sub(&mut trie, "foo.bar", "g");

        trie.remove_sub("nothing.here".into(), "g".into(), 1);

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        assert_eq!(count(&mut rx), 1);
    }

    #[test]
    fn disconnect_clears_a_connection_from_every_topic() {
        let mut trie = Trie::new();
        let (tx, mut rx) = chan();
        trie.add_sub("foo.bar".into(), "g".into(), tx.clone(), 1, 7);
        trie.add_sub("other.topic".into(), "g".into(), tx, 1, 7);

        trie.remove_conn(7);

        assert_eq!(trie.send_message("foo.bar".into(), payload()), 0);
        assert_eq!(trie.send_message("other.topic".into(), payload()), 0);
        assert_eq!(count(&mut rx), 0);
    }

    // ------------------------------------------------------- pruning / memory

    #[test]
    fn unsubscribing_prunes_the_nodes_it_emptied() {
        let mut trie = Trie::new();
        sub(&mut trie, "foo.bar.baz", "g");
        assert_eq!(trie.nodes.len(), 4);

        trie.remove_sub("foo.bar.baz".into(), "g".into(), 1);

        // The arena keeps its slots, but every node below the root is freed.
        assert_eq!(trie.free.len(), 3);
        assert_eq!(trie.send_message("foo.bar.baz".into(), payload()), 0);
    }

    #[test]
    fn pruning_stops_at_a_node_that_still_has_subscribers() {
        let mut trie = Trie::new();
        // Receivers must stay alive: `try_send` fails against a dropped one.
        let _keep_a = sub(&mut trie, "foo.bar", "g1");
        let _keep_b = sub(&mut trie, "foo.bar.baz", "g2");

        trie.remove_sub("foo.bar.baz".into(), "g2".into(), 1);

        // `baz` is freed; `foo` and `bar` stay reachable because `bar` still
        // holds g1.
        assert_eq!(trie.free.len(), 1);
        assert_eq!(trie.send_message("foo.bar".into(), payload()), 1);
        assert_eq!(trie.send_message("foo.bar.baz".into(), payload()), 0);
    }

    #[test]
    fn disconnect_reclaims_the_nodes_its_subscriptions_held() {
        let mut trie = Trie::new();
        let (tx, _rx) = chan();

        for i in 0..20 {
            trie.add_sub(format!("a{i}.b.c"), "g".into(), tx.clone(), 1, 1);
        }
        // root + 20 x (aN, b, c)
        assert_eq!(trie.nodes.len(), 61);

        trie.remove_conn(1);

        // Every node but the root is detached and back on the free list.
        assert_eq!(trie.nodes.len(), 61, "arena keeps its slots");
        assert_eq!(trie.free.len(), 60);
    }

    #[test]
    fn disconnect_keeps_branches_that_other_connections_still_use() {
        let mut trie = Trie::new();
        let (tx_a, _rx_a) = chan();
        let (tx_b, _rx_b) = chan();

        // Both connections subscribe under a shared prefix, so pruning `a`
        // must not take `b` with it.
        trie.add_sub("shared.leaf.one".into(), "g".into(), tx_a, 1, 10);
        trie.add_sub("shared.leaf.two".into(), "g".into(), tx_b, 1, 20);
        // root + shared + leaf + one + two
        assert_eq!(trie.nodes.len(), 5);

        trie.remove_conn(10);

        // root, shared, leaf, two  (the `one` branch is gone)
        assert_eq!(trie.free.len(), 1);
        assert_eq!(trie.send_message("shared.leaf.one".into(), payload()), 0);
        assert_eq!(trie.send_message("shared.leaf.two".into(), payload()), 1);
    }

    #[test]
    fn disconnect_keeps_a_sibling_group_on_the_same_node() {
        let mut trie = Trie::new();
        let (tx_a, _rx_a) = chan();
        let (tx_b, _rx_b) = chan();

        // Same topic, two groups. Dropping one connection empties `g1` but must
        // leave `g2` — and the node — intact.
        trie.add_sub("multi".into(), "g1".into(), tx_a, 1, 10);
        trie.add_sub("multi".into(), "g2".into(), tx_b, 1, 20);
        assert_eq!(trie.nodes.len(), 2);

        trie.remove_conn(10);

        assert_eq!(trie.free.len(), 0, "the node is still in use");
        assert_eq!(trie.send_message("multi".into(), payload()), 1);
    }

    #[test]
    fn churn_of_connect_and_disconnect_does_not_grow_the_arena() {
        let mut trie = Trie::new();
        let (tx, _rx) = chan();

        for i in 0..50u32 {
            let conn = i as usize + 1;
            trie.add_sub("churn.topic".into(), "g".into(), tx.clone(), 1, conn);
            trie.remove_conn(conn);
        }

        // root + churn + topic, and the high-water mark is not exceeded.
        assert_eq!(trie.nodes.len(), 3);
    }

    #[test]
    fn churn_reuses_freed_slots_instead_of_growing_the_arena() {
        let mut trie = Trie::new();

        // One cycle needs root + churn + topic; every later cycle reuses them.
        for _ in 0..50 {
            trie.add_sub("churn.topic".into(), "g".into(), chan().0, 1, 1);
            trie.remove_sub("churn.topic".into(), "g".into(), 1);
        }

        assert_eq!(trie.nodes.len(), 3, "arena must not grow with iterations");
    }
}
