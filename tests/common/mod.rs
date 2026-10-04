use std::time::Duration;

use stan::common::shutdown::Shutdown;
use stan::config::config::{AppConfig, Config};
use stan::network::types::ResponseType;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Command bytes, mirroring `read_frame`.
pub const CMD_INFO: u8 = 1;
pub const CMD_PING: u8 = 2;
pub const CMD_SUB: u8 = 3;
pub const CMD_PUB: u8 = 4;
pub const CMD_UNSUB: u8 = 5;

/// A test client that speaks the wire protocol over a real socket.
///
/// The encoders here are written independently of `to_bytes` rather than
/// reusing it, so a change to the server's serialisation can't quietly make a
/// test agree with a bug.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> Self {
        Self::try_connect(addr).await.expect("connect")
    }

    /// Unlike [`Client::connect`], a refused connection is a result, not a
    /// panic, so tests can assert the server is no longer listening.
    pub async fn try_connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// `[cmd][sub_id:1][topic_len:4][group_len:4][topic][group]`
    pub async fn subscribe(&mut self, topic: &str, group: &str, sub_id: u8) {
        let topic = topic.as_bytes();
        let group = group.as_bytes();
        let mut buf = Vec::new();
        buf.push(CMD_SUB);
        buf.push(sub_id);
        buf.extend_from_slice(&(topic.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(group.len() as u32).to_be_bytes());
        buf.extend_from_slice(topic);
        buf.extend_from_slice(group);
        self.stream.write_all(&buf).await.expect("write sub");
    }

    pub async fn unsubscribe(&mut self, topic: &str, group: &str, sub_id: u8) {
        let topic = topic.as_bytes();
        let group = group.as_bytes();
        let mut buf = Vec::new();
        buf.push(CMD_UNSUB);
        buf.push(sub_id);
        buf.extend_from_slice(&(topic.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(group.len() as u32).to_be_bytes());
        buf.extend_from_slice(topic);
        buf.extend_from_slice(group);
        self.stream.write_all(&buf).await.expect("write unsub");
    }

    /// `[cmd][topic_len:4][payload_len:4][topic][payload][timestamp:8]`
    pub async fn publish(&mut self, topic: &str, payload: &[u8], timestamp: u64) {
        let topic = topic.as_bytes();
        let mut buf = Vec::new();
        buf.push(CMD_PUB);
        buf.extend_from_slice(&(topic.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(topic);
        buf.extend_from_slice(payload);
        buf.extend_from_slice(&timestamp.to_be_bytes());
        self.stream.write_all(&buf).await.expect("write pub");
    }

    pub async fn ping(&mut self) {
        self.stream
            .write_all(&[CMD_PING])
            .await
            .expect("write ping");
    }

    pub async fn info(&mut self) {
        self.stream
            .write_all(&[CMD_INFO])
            .await
            .expect("write info");
    }

    /// A SUB/UNSUB header claiming lengths, with no topic or group body.
    ///
    /// `topic_len` and `group_len` are independent so a test can blow up
    /// either one.
    pub async fn claim_sub_lengths(&mut self, sub_id: u8, topic_len: u32, group_len: u32) {
        let mut buf = vec![CMD_SUB, sub_id];
        buf.extend_from_slice(&topic_len.to_be_bytes());
        buf.extend_from_slice(&group_len.to_be_bytes());
        self.stream.write_all(&buf).await.expect("write sub header");
    }

    /// `[token_len:4][token]` — the auth frame the server expects first.
    pub async fn authenticate(&mut self, token: &[u8]) {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(token.len() as u32).to_be_bytes());
        buf.extend_from_slice(token);
        self.stream.write_all(&buf).await.expect("write auth");
    }

    /// A length prefix claiming more than any legal token, with no body.
    pub async fn claim_token_len(&mut self, claimed: u32) {
        self.stream
            .write_all(&claimed.to_be_bytes())
            .await
            .expect("write len");
    }

    /// Writes raw bytes, for malformed-frame tests.
    pub async fn write_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("write raw");
    }

    /// Reads one `[kind:1]` + body frame. Returns the kind and its body.
    pub async fn read_response(&mut self) -> (u8, Vec<u8>) {
        let mut kind = [0u8; 1];
        self.stream.read_exact(&mut kind).await.expect("read kind");

        let body = match kind[0] {
            k if k == ResponseType::PONG as u8 => Vec::new(),
            k if k == ResponseType::INFO as u8 => {
                // Four u32 lengths, then the fixed fields (port u32,
                // max_payload u64, max_control_line u32, two bools), then the
                // four string bodies.
                let mut head = [0u8; 4];
                let mut lens = Vec::new();
                for _ in 0..4 {
                    self.stream.read_exact(&mut head).await.expect("len");
                    lens.push(u32::from_be_bytes(head) as usize);
                }
                let mut fixed = [0u8; 4 + 8 + 4 + 1 + 1];
                self.stream.read_exact(&mut fixed).await.expect("fixed");
                let mut out = fixed.to_vec();
                for n in lens {
                    let mut body = vec![0u8; n];
                    self.stream.read_exact(&mut body).await.expect("body");
                    out.extend_from_slice(&body);
                }
                out
            }
            k if k == ResponseType::MSG as u8 => {
                // sub_id:1, id:8, timestamp:8, payload_len:4, payload
                let mut fixed = [0u8; 1 + 8 + 8 + 4];
                self.stream.read_exact(&mut fixed).await.expect("msg head");
                let payload_len = u32::from_be_bytes(fixed[17..21].try_into().unwrap()) as usize;
                let mut payload = vec![0u8; payload_len];
                self.stream.read_exact(&mut payload).await.expect("payload");
                fixed
                    .to_vec()
                    .into_iter()
                    .chain(payload)
                    .collect::<Vec<u8>>()
            }
            k if k == ResponseType::Err as u8 => {
                // `[code]` plus, for some codes, a length-prefixed context.
                let mut code = [0u8; 1];
                self.stream.read_exact(&mut code).await.expect("code");
                let mut out = code.to_vec();
                if err_carries_context(code[0]) {
                    let mut len = [0u8; 4];
                    self.stream.read_exact(&mut len).await.expect("ctx len");
                    let n = u32::from_be_bytes(len) as usize;
                    let mut ctx = vec![0u8; n];
                    self.stream.read_exact(&mut ctx).await.expect("ctx");
                    out.extend_from_slice(&ctx);
                }
                out
            }
            other => panic!("unknown response kind {other:#04x}"),
        };

        (kind[0], body)
    }

    /// Reads the next message, asserting it is a MSG and decoding it.
    pub async fn read_message(&mut self) -> DecodedMessage {
        let (kind, body) = self.read_response().await;
        assert_eq!(
            kind,
            ResponseType::MSG as u8,
            "expected MSG, got kind {kind:#04x}"
        );

        let sub_id = body[0];
        let id = u64::from_be_bytes(body[1..9].try_into().unwrap());
        let timestamp = u64::from_be_bytes(body[9..17].try_into().unwrap());
        let payload_len = u32::from_be_bytes(body[17..21].try_into().unwrap()) as usize;
        let payload = body[21..21 + payload_len].to_vec();

        DecodedMessage {
            sub_id,
            id,
            timestamp,
            payload,
        }
    }

    /// True once the server has closed the connection.
    pub async fn is_closed(&mut self) -> bool {
        let mut buf = [0u8; 1];
        matches!(self.stream.read(&mut buf).await, Ok(0) | Err(_))
    }

    /// True if the connection is still usable. A read on an idle-but-open
    /// socket never returns, so absence of EOF within the window counts as open.
    pub async fn still_open(&mut self) -> bool {
        let mut buf = [0u8; 1];
        tokio::time::timeout(Duration::from_millis(300), self.stream.read(&mut buf))
            .await
            .is_err()
    }

    /// Asserts the server sends nothing within a short window.
    pub async fn expect_silence(&mut self) {
        let mut buf = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(250), self.stream.read(&mut buf))
                .await
                .is_err(),
            "expected no response"
        );
    }

    /// Half-closes so a server blocked reading the rest of a frame sees EOF.
    pub async fn half_close(&mut self) {
        let _ = self.stream.shutdown().await;
    }
}

pub struct DecodedMessage {
    pub sub_id: u8,
    pub id: u64,
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

/// Response kinds, mirroring `ResponseType`.
pub const PONG: u8 = 0x05;
pub const INFO: u8 = 0x04;
pub const ERR: u8 = 0x02;

/// Mirrors `AppError::context`: only these codes are followed by a context
/// string. `AuthError`, `MaxPayloadErr` and `MaxTokenLengthError` are bare.
fn err_carries_context(code: u8) -> bool {
    matches!(
        code,
        x if x == stan::network::types::ErrorCode::InvalidTopic as u8
            || x == stan::network::types::ErrorCode::WildcardInPublish as u8
            || x == stan::network::types::ErrorCode::MaxArtifactsErr as u8
    )
}

/// Error codes, mirroring `ErrorCode`.
pub const ERR_MAX_PAYLOAD: u8 = 0x02;
pub const ERR_TOKEN_TOO_LONG: u8 = 0x06;
pub const ERR_AUTH: u8 = 0x01;
pub const ERR_MAX_ARTIFACTS: u8 = 0x03;

/// A config with auth off, so tests don't need the token env var.
pub fn test_config() -> AppConfig {
    AppConfig {
        token: None,
        config: Config {
            server_id: "test-server".into(),
            version: "0.0.0".into(),
            runtime: "test".into(),
            host: "127.0.0.1".into(),
            port: 0,
            max_payload: 1024,
            max_control_line: 256,
            tls_required: false,
            auth_required: false,
        },
    }
}

/// The same server, but with `token` required on every connection.
pub fn test_config_with_token(token: &str) -> AppConfig {
    AppConfig {
        token: Some(token.to_string()),
        config: Config {
            auth_required: true,
            ..test_config().config
        },
    }
}

/// Spins up a server on an OS-assigned port. Returns its address and a handle.
pub async fn spawn_server() -> (String, Shutdown, tokio::task::JoinHandle<()>) {
    let shutdown = Shutdown::new();
    let server_shutdown = shutdown.clone();
    let listener = stan::network::net::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();

    let handle = tokio::spawn(async move {
        let _ = stan::network::net::serve(listener, test_config(), server_shutdown).await;
    });

    (addr, shutdown, handle)
}

/// Spins up a server that requires `token` before any command is accepted.
pub async fn spawn_server_with_token(
    token: &str,
) -> (String, Shutdown, tokio::task::JoinHandle<()>) {
    let shutdown = Shutdown::new();
    let server_shutdown = shutdown.clone();
    let listener = stan::network::net::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let config = test_config_with_token(token);

    let handle = tokio::spawn(async move {
        let _ = stan::network::net::serve(listener, config, server_shutdown).await;
    });

    (addr, shutdown, handle)
}

/// Bounds a future by a timeout, so a hung drain fails fast instead of
/// hanging the whole test run. Returns the future's output.
pub async fn within<F: std::future::Future>(
    dur: std::time::Duration,
    what: &str,
    f: F,
) -> F::Output {
    tokio::time::timeout(dur, f)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish in time"))
}
