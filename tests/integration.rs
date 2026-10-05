//! End-to-end tests over real loopback sockets.
//!
//! The client in `common` re-implements the wire format rather than reusing the
//! server's `to_bytes`, so a serialisation bug can't be masked by a test that
//! agrees with it.

mod common;

use common::{CMD_PUB, Client, spawn_server, spawn_server_with_token, within};
use std::time::Duration;
use tokio::time::sleep;

const T: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------- pub / sub

#[tokio::test]
async fn subscriber_receives_a_published_message() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("foo.bar", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("foo.bar", b"hello", 42).await;

    let msg = within(T, "msg delivery", sub.read_message()).await;
    assert_eq!(msg.payload, b"hello");
    assert_eq!(msg.timestamp, 42);
    assert_eq!(msg.sub_id, 1);
}

#[tokio::test]
async fn wildcard_subscription_matches_deeper_topics() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("foo.>", "g", 7).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("foo.bar.baz", b"deep", 1).await;

    let msg = within(T, "wildcard delivery", sub.read_message()).await;
    assert_eq!(msg.payload, b"deep");
    assert_eq!(msg.sub_id, 7);
}

#[tokio::test]
async fn non_matching_topic_delivers_nothing() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("foo.bar", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("other.topic", b"nope", 1).await;

    // Nothing should arrive, and the subscriber must still be connected.
    assert!(
        sub.still_open().await,
        "subscriber connection must survive an unmatched publish"
    );
}

#[tokio::test]
async fn queue_group_load_balances_across_connections() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut a = Client::connect(&addr).await;
    a.subscribe("jobs", "workers", 1).await;
    let mut b = Client::connect(&addr).await;
    b.subscribe("jobs", "workers", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    for i in 0..4u64 {
        pubr.publish("jobs", format!("job-{i}").as_bytes(), i).await;
    }

    // Round-robin over two connections: each gets exactly two.
    for _ in 0..2 {
        within(T, "a msg", a.read_message()).await;
    }
    for _ in 0..2 {
        within(T, "b msg", b.read_message()).await;
    }
}

#[tokio::test]
async fn separate_groups_both_receive() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut a = Client::connect(&addr).await;
    a.subscribe("evt", "g1", 1).await;
    let mut b = Client::connect(&addr).await;
    b.subscribe("evt", "g2", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("evt", b"both", 1).await;

    assert_eq!(within(T, "a", a.read_message()).await.payload, b"both");
    assert_eq!(within(T, "b", b.read_message()).await.payload, b"both");
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("foo.bar", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("foo.bar", b"first", 1).await;
    within(T, "first", sub.read_message()).await;

    sub.unsubscribe("foo.bar", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    pubr.publish("foo.bar", b"second", 2).await;
    sleep(Duration::from_millis(100)).await;

    assert!(
        sub.still_open().await,
        "unsubscribe must not close the connection"
    );
}

#[tokio::test]
async fn publisher_can_send_many_messages_on_one_connection() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("stream", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    for i in 0..5u64 {
        pubr.publish("stream", format!("m{i}").as_bytes(), i).await;
    }

    let mut last_id = None;
    for i in 0..5u64 {
        let msg = within(T, "stream msg", sub.read_message()).await;
        assert_eq!(msg.payload, format!("m{i}").into_bytes());
        // The actor stamps a monotonic id, so it must advance.
        if let Some(prev) = last_id {
            assert!(msg.id > prev, "id {} not after {prev}", msg.id);
        }
        last_id = Some(msg.id);
    }
}

// ------------------------------------------------------------- ping / info

#[tokio::test]
async fn ping_is_answered_with_pong() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.ping().await;

    let (kind, _) = within(T, "pong", client.read_response()).await;
    assert_eq!(kind, common::PONG);
}

#[tokio::test]
async fn info_returns_the_server_config() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.info().await;

    let (kind, body) = within(T, "info", client.read_response()).await;
    assert_eq!(kind, common::INFO);
    assert!(body.windows(11).any(|w| w == b"test-server"));
}

// ------------------------------------------------------------------ errors

#[tokio::test]
async fn publishing_a_wildcard_topic_is_rejected() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("foo.*", b"bad", 1).await;

    let (kind, _) = within(T, "err", pubr.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert!(within(T, "closed", pubr.is_closed()).await);
}

#[tokio::test]
async fn oversized_topic_is_rejected_without_allocating_it() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut pubr = Client::connect(&addr).await;
    // Claim a 4 GB topic while sending no topic bytes at all. A server that
    // allocated before validating would OOM here.
    let mut frame = vec![CMD_PUB];
    frame.extend_from_slice(&u32::MAX.to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(&0u64.to_be_bytes());
    pubr.write_raw(&frame).await;

    let (kind, _) = within(T, "err", pubr.read_response()).await;
    assert_eq!(kind, common::ERR);
}

#[tokio::test]
async fn oversized_payload_is_rejected() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut pubr = Client::connect(&addr).await;
    let topic = b"foo";
    let mut frame = vec![CMD_PUB];
    frame.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    frame.extend_from_slice(&u32::MAX.to_be_bytes());
    frame.extend_from_slice(topic);
    frame.extend_from_slice(&0u64.to_be_bytes());
    pubr.write_raw(&frame).await;

    let (kind, body) = within(T, "err", pubr.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_MAX_PAYLOAD);
}

#[tokio::test]
async fn an_unknown_command_closes_the_connection() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.write_raw(&[0xFF]).await;

    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn a_truncated_frame_closes_the_connection() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // A PUB header promising 10 bytes of topic, with no topic sent.
    let mut frame = vec![CMD_PUB];
    frame.extend_from_slice(&10u32.to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    client.write_raw(&frame).await;
    client.half_close().await;

    assert!(within(T, "closed", client.is_closed()).await);
}

// ---------------------------------------------------------------- shutdown

#[tokio::test]
async fn shutdown_closes_idle_subscriber_connections() {
    let (addr, shutdown, server) = spawn_server().await;

    // A subscriber never sends anything after its SUB, so it is parked in
    // read_frame forever. Only the shutdown arm can end it.
    let mut sub = Client::connect(&addr).await;
    sub.subscribe("idle", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    shutdown.signal();

    within(T, "subscriber sees close", sub.is_closed()).await;
    within(T, "serve returns", server).await.unwrap();
}

#[tokio::test]
async fn shutdown_completes_with_no_connections_at_all() {
    let (_addr, shutdown, server) = spawn_server().await;

    shutdown.signal();

    within(T, "serve returns", server).await.unwrap();
}

#[tokio::test]
async fn shutdown_completes_with_many_connections() {
    let (addr, shutdown, server) = spawn_server().await;

    let mut clients = Vec::new();
    for _ in 0..20 {
        let mut c = Client::connect(&addr).await;
        c.subscribe("bulk", "g", 1).await;
        clients.push(c);
    }
    sleep(Duration::from_millis(100)).await;

    shutdown.signal();

    within(T, "serve returns", server).await.unwrap();
    for c in clients.iter_mut() {
        assert!(within(T, "client closed", c.is_closed()).await);
    }
}

#[tokio::test]
async fn no_new_connections_are_accepted_after_shutdown() {
    let (addr, shutdown, server) = spawn_server().await;

    shutdown.signal();
    within(T, "serve returns", server).await.unwrap();

    // The listener is gone, so no connection may be established. How that
    // failure surfaces is platform-dependent: Unix sends an RST and `connect`
    // returns ECONNREFUSED, while Windows Firewall drops the SYN silently and
    // the attempt hangs until it times out. Both mean nothing is listening, so
    // only a *successful* connect is a failure.
    let result = tokio::time::timeout(Duration::from_millis(500), Client::try_connect(&addr)).await;
    if let Ok(Ok(_)) = result {
        panic!("connect should not succeed after shutdown");
    }
}

// ------------------------------------------------------------------- auth

#[tokio::test]
async fn a_correct_token_is_accepted() {
    let (addr, _shutdown, _server) = spawn_server_with_token("s3cret").await;

    let mut client = Client::connect(&addr).await;
    client.authenticate(b"s3cret").await;

    // The connection survives the handshake, so PING still works.
    client.ping().await;
    let (kind, _) = within(T, "pong", client.read_response()).await;
    assert_eq!(kind, common::PONG);
}

#[tokio::test]
async fn a_wrong_token_is_rejected() {
    let (addr, _shutdown, _server) = spawn_server_with_token("s3cret").await;

    let mut client = Client::connect(&addr).await;
    client.authenticate(b"wrong").await;

    let (kind, body) = within(T, "err", client.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_AUTH);
    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn commands_before_authenticating_are_rejected() {
    let (addr, _shutdown, _server) = spawn_server_with_token("s3cret").await;

    let mut client = Client::connect(&addr).await;
    client.ping().await;

    // The lone PING byte is consumed as part of the auth frame's length
    // prefix, so the command never runs and no PONG comes back.
    client.expect_silence().await;
}

#[tokio::test]
async fn an_oversized_token_length_is_rejected_without_reading_it() {
    let (addr, _shutdown, _server) = spawn_server_with_token("s3cret").await;

    let mut client = Client::connect(&addr).await;
    // Claim 4 GB and send nothing. A server that allocated before validating
    // would OOM here.
    client.claim_token_len(u32::MAX).await;

    let (kind, body) = within(T, "err", client.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_TOKEN_TOO_LONG);
}

#[tokio::test]
async fn a_token_at_the_limit_is_accepted() {
    let token = "a".repeat(1024);
    let (addr, _shutdown, _server) = spawn_server_with_token(&token).await;

    let mut client = Client::connect(&addr).await;
    client.authenticate(token.as_bytes()).await;
    client.ping().await;

    let (kind, _) = within(T, "pong", client.read_response()).await;
    assert_eq!(kind, common::PONG);
}

// ------------------------------------- oversized lengths on the SUB path

#[tokio::test]
async fn an_oversized_topic_on_subscribe_is_rejected_without_reading_it() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // Claim a 4 GB topic and send no topic bytes. The check has to happen
    // against the length prefix, before any allocation.
    client.claim_sub_lengths(1, u32::MAX, 0).await;

    let (kind, body) = within(T, "err", client.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_MAX_ARTIFACTS);
}

#[tokio::test]
async fn an_oversized_group_on_subscribe_is_rejected_without_reading_it() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // Topic length is fine; the group is the one that blows up.
    client.claim_sub_lengths(1, 4, u32::MAX).await;

    let (kind, body) = within(T, "err", client.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_MAX_ARTIFACTS);
}

#[tokio::test]
async fn an_oversized_topic_on_unsubscribe_is_rejected_without_reading_it() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    // Same guard on the shared parser, so it must cover UNSUB too.
    let mut frame = vec![common::CMD_UNSUB, 1];
    frame.extend_from_slice(&u32::MAX.to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    client.write_raw(&frame).await;

    let (kind, body) = within(T, "err", client.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_MAX_ARTIFACTS);
}

#[tokio::test]
async fn a_topic_at_the_control_line_limit_is_accepted_on_subscribe() {
    let (addr, _shutdown, _server) = spawn_server().await;

    // test_config sets max_control_line to 256.
    let topic = "t".repeat(256);
    let mut client = Client::connect(&addr).await;
    client.subscribe(&topic, "g", 1).await;

    // Accepted, so the connection stays usable.
    client.ping().await;
    let (kind, _) = within(T, "pong", client.read_response()).await;
    assert_eq!(kind, common::PONG);
}

#[tokio::test]
async fn a_stalled_token_frame_times_out() {
    let (addr, _shutdown, _server) = spawn_server_with_token("s3cret").await;

    let mut client = Client::connect(&addr).await;
    // Three of the four length-prefix bytes, then nothing. The server must not
    // wait on this forever.
    client.write_raw(&[0x00, 0x00, 0x00]).await;

    // Costs a real second: the timeout is a wall-clock constant, and paused time
    // does not advance while a task is parked on live socket I/O.
    let (kind, body) = within(T, "timeout", client.read_response()).await;
    assert_eq!(kind, common::ERR);
    assert_eq!(body[0], common::ERR_AUTH_TIMEOUT);
    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn a_slow_but_valid_token_frame_still_authenticates() {
    let (addr, _shutdown, _server) = spawn_server_with_token("s3cret").await;

    let mut client = Client::connect(&addr).await;
    // Split the frame and take longer than a scheduler hiccup would, but well
    // inside the 1s budget, so a tight deadline would show up as a failure here.
    client.write_raw(&(6u32).to_be_bytes()).await;
    sleep(Duration::from_millis(250)).await;
    client.write_raw(b"s3cret").await;

    client.ping().await;
    let (kind, _) = within(T, "pong", client.read_response()).await;
    assert_eq!(kind, common::PONG);
}

#[tokio::test]
async fn a_disconnect_closes_the_connection() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut client = Client::connect(&addr).await;
    client.disconnect().await;

    assert!(within(T, "closed", client.is_closed()).await);
}

#[tokio::test]
async fn a_disconnect_ends_delivery_to_that_subscriber() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("bye.world", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("bye.world", b"first", 1).await;
    within(T, "first", sub.read_message()).await;

    sub.disconnect().await;
    assert!(within(T, "sub closed", sub.is_closed()).await);

    // The subscriber is gone, so the next publish matches nothing. The
    // publisher is unaffected and stays connected.
    pubr.publish("bye.world", b"second", 2).await;
    pubr.ping().await;
    let (kind, _) = within(T, "pong", pubr.read_response()).await;
    assert_eq!(kind, common::PONG);
}

#[tokio::test]
async fn a_disconnect_removes_every_subscription_on_that_connection() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("one", "g", 1).await;
    sub.subscribe("two", "g", 2).await;
    sub.subscribe("three", "g1", 3).await;
    sleep(Duration::from_millis(50)).await;

    sub.disconnect().await;
    assert!(within(T, "closed", sub.is_closed()).await);
    // Give the actor time to process the cleanup event.
    sleep(Duration::from_millis(100)).await;

    // A surviving subscription would leave a dead writer in its group, and
    // round-robin would then route some publishes to a channel nobody drains.
    // So: add one replacement subscriber per affected topic, and require every
    // one of them to receive every message. A leftover entry shows up as a
    // message that goes missing.
    let mut replacements = Vec::new();
    for (topic, group, sub_id) in [("one", "g", 9), ("two", "g", 10), ("three", "g1", 11)] {
        let mut client = Client::connect(&addr).await;
        client.subscribe(topic, group, sub_id).await;
        replacements.push((topic.to_string(), sub_id, client));
    }
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    const ROUNDS: usize = 3;
    for round in 0..ROUNDS {
        for (topic, _, _) in &replacements {
            pubr.publish(topic, b"delivered", round as u64).await;
        }
    }

    for (topic, sub_id, client) in replacements.iter_mut() {
        for round in 0..ROUNDS {
            let msg = within(T, "msg", client.read_message()).await;
            assert_eq!(
                msg.payload, b"delivered",
                "{topic} round {round} was not delivered"
            );
            assert_eq!(
                msg.sub_id, *sub_id,
                "{topic} round {round} carried the wrong sub_id"
            );
        }
    }
}

#[tokio::test]
async fn other_subscribers_are_unaffected_by_a_disconnect() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut leaver = Client::connect(&addr).await;
    leaver.subscribe("shared", "g", 1).await;
    let mut stayer = Client::connect(&addr).await;
    stayer.subscribe("shared", "g", 2).await;
    sleep(Duration::from_millis(50)).await;

    leaver.disconnect().await;
    assert!(within(T, "leaver closed", leaver.is_closed()).await);
    sleep(Duration::from_millis(100)).await;

    let mut pubr = Client::connect(&addr).await;
    for _ in 0..3 {
        pubr.publish("shared", b"still here", 1).await;
    }

    // The group now has one member, so it must receive all three.
    for _ in 0..3 {
        let msg = within(T, "msg", stayer.read_message()).await;
        assert_eq!(msg.payload, b"still here");
    }
}

#[tokio::test]
async fn a_publisher_can_disconnect_after_publishing() {
    let (addr, _shutdown, _server) = spawn_server().await;

    let mut sub = Client::connect(&addr).await;
    sub.subscribe("last.words", "g", 1).await;
    sleep(Duration::from_millis(50)).await;

    let mut pubr = Client::connect(&addr).await;
    pubr.publish("last.words", b"bye", 1).await;
    within(T, "msg", sub.read_message()).await;

    pubr.disconnect().await;
    assert!(within(T, "closed", pubr.is_closed()).await);
}
