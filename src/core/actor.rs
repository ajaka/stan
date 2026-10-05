use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::core::{group::MessagePayload, tree::Trie, types::WriterMessage};

pub enum Event {
    SUBSCRIBE {
        topic: String,
        group: String,
        sender: Sender<WriterMessage>,
        sub_id: u8,
        conn_id: usize,
    },
    PUBLISH {
        topic: String,
        payload: Vec<u8>,
        timestamp: u64,
    },
    UNSUBSCRIBE {
        topic: String,
        group: String,
        conn_id: usize,
    },
    DISCONNECT {
        conn_id: usize,
    },
}

pub struct Actor {}

impl Actor {
    pub async fn init(mut actor_receiver: Receiver<Event>) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            let mut tree = Trie::new();
            let mut id = 0;

            while let Some(e) = actor_receiver.recv().await {
                match e {
                    Event::SUBSCRIBE {
                        topic,
                        group,
                        sender,
                        sub_id,
                        conn_id,
                    } => {
                        tree.add_sub(topic, group, sender, sub_id, conn_id);
                    }
                    Event::PUBLISH {
                        topic,
                        payload,
                        timestamp,
                    } => {
                        let msg = MessagePayload {
                            payload,
                            timestamp,
                            id,
                        };
                        id += 1;
                        tree.send_message(topic, msg);
                    }
                    Event::UNSUBSCRIBE {
                        topic,
                        group,
                        conn_id,
                    } => {
                        tree.remove_sub(topic, group, conn_id);
                    }
                    Event::DISCONNECT { conn_id } => {
                        tree.remove_conn(conn_id);
                    }
                }
            }
        })
    }
}
