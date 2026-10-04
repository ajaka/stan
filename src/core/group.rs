use tokio::sync::mpsc::Sender;

use crate::core::types::{Message, WriterMessage};

#[derive(Debug, Clone)]
pub struct MessagePayload {
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub id: u64,
}

pub struct Connections {
    pub sender: Sender<WriterMessage>,
    pub id: usize,
    pub sub_id: u8,
    pub conn_id: usize,
}

pub struct Group {
    connections: Vec<Connections>,
    next: usize,
    id: usize,
}

impl Group {
    pub fn new() -> Self {
        Self {
            connections: Vec::new(),
            next: 0,
            id: 1,
        }
    }

    pub fn add_conn(&mut self, sender: Sender<WriterMessage>, sub_id: u8, conn_id: usize) {
        self.remove_conn(conn_id);
        let new_conn = Connections {
            sender,
            id: self.id,
            sub_id,
            conn_id,
        };
        self.id += 1;
        self.connections.push(new_conn);
    }

    pub fn remove_conn(&mut self, conn_id: usize) {
        self.connections.retain(|c| c.conn_id != conn_id);
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    pub fn len(&self) -> usize {
        self.connections.len()
    }

    pub fn send(&mut self, msg: &MessagePayload) -> bool {
        let len = self.connections.len();
        if len == 0 {
            return false;
        }

        let index = self.next % len;
        self.next = (index + 1) % len;

        let conn = &self.connections[index];
        let message = Message {
            payload: msg.payload.clone(),
            timestamp: msg.timestamp,
            id: msg.id,
            sub_id: conn.sub_id,
        };

        conn.sender
            .try_send(WriterMessage::Msg { m: message })
            .is_ok()
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

    fn drained(rx: &mut Receiver<WriterMessage>) -> usize {
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        n
    }

    #[test]
    fn empty_group_delivers_nothing() {
        let mut group = Group::new();
        assert_eq!(group.len(), 0);
        assert!(group.is_empty());
        assert!(!group.send(&payload()));
    }

    #[test]
    fn round_robin_cycles_through_every_connection() {
        let mut group = Group::new();
        let (tx_a, mut rx_a) = chan();
        let (tx_b, mut rx_b) = chan();
        group.add_conn(tx_a, 1, 10);
        group.add_conn(tx_b, 1, 20);

        for _ in 0..4 {
            assert!(group.send(&payload()));
        }

        assert_eq!(drained(&mut rx_a), 2);
        assert_eq!(drained(&mut rx_b), 2);
    }

    #[test]
    fn round_robin_wraps_without_growing_the_cursor() {
        let mut group = Group::new();
        let (tx_a, mut rx_a) = chan();
        group.add_conn(tx_a, 1, 10);

        for _ in 0..10 {
            group.send(&payload());
        }

        // The cursor stays in range, so nothing is lost.
        assert_eq!(drained(&mut rx_a), 10);
    }

    /// Regression: `retain` used to drop connections matching on `sub_id`
    /// alone, so two clients sharing `sub_id: 1` deleted each other.
    #[test]
    fn shared_sub_id_does_not_evict_a_different_connection() {
        let mut group = Group::new();
        let (tx_a, _rx_a) = chan();
        let (tx_b, _rx_b) = chan();
        group.add_conn(tx_a, 1, 10);
        group.add_conn(tx_b, 1, 20);
        assert_eq!(group.len(), 2);

        group.remove_conn(10);

        assert_eq!(group.len(), 1);
    }

    #[test]
    fn resubscribing_replaces_the_existing_entry() {
        let mut group = Group::new();
        let (tx_a, mut rx_a) = chan();
        group.add_conn(tx_a.clone(), 1, 10);

        group.add_conn(tx_a, 2, 10);

        assert_eq!(group.len(), 1);
        group.send(&payload());
        match rx_a.try_recv().expect("one message") {
            WriterMessage::Msg { m } => assert_eq!(m.sub_id, 2, "newest sub_id wins"),
            _ => panic!("expected a message"),
        }
    }

    #[test]
    fn removing_the_last_connection_empties_the_group() {
        let mut group = Group::new();
        let (tx, _rx) = chan();
        group.add_conn(tx, 1, 10);
        assert!(!group.is_empty());

        group.remove_conn(10);

        assert!(group.is_empty());
        assert!(!group.send(&payload()));
    }

    #[test]
    fn removing_an_unknown_connection_is_a_no_op() {
        let mut group = Group::new();
        let (tx, _rx) = chan();
        group.add_conn(tx, 1, 10);

        group.remove_conn(999);

        assert_eq!(group.len(), 1);
    }

    #[test]
    fn send_fails_when_the_receiver_is_gone() {
        let mut group = Group::new();
        let (tx, rx) = chan();
        group.add_conn(tx, 1, 10);
        drop(rx);

        assert!(!group.send(&payload()));
    }
}
