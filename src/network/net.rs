use std::sync::{Arc, atomic::AtomicUsize};

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, tcp::OwnedReadHalf},
    sync::mpsc,
    task::JoinSet,
};

use crate::{
    common::shutdown::Shutdown,
    common::utils::{Wildcards, split, validate_topic},
    config::config::{AppConfig, Config},
    core::{
        actor::{Actor, Event},
        types::{AppError, WriterMessage},
    },
    network::{
        auth::handshake,
        types::{AppEvent, FrameError, ResponseType},
    },
};

pub async fn bind(addr: &str) -> Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    println!("Pub/Sub Server listening on {}", local);
    Ok(listener)
}

pub async fn serve(listener: TcpListener, config: AppConfig, shutdown: Shutdown) -> Result<()> {
    let id = AtomicUsize::new(0);
    let app = Arc::new(config);
    let (actor_sender, actor_receiver) = mpsc::channel(100);
    let actor_handle = Actor::init(actor_receiver).await;
    let mut connections = JoinSet::new();

    loop {
        let socket = tokio::select! {
            biased;
            _ = shutdown.wait() => break,
            accepted = listener.accept() => accepted?.0,
        };

        let token = app.token.clone();
        let conn_id = id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cloned_app = app.clone();
        let task_sender = actor_sender.clone();
        let conn_shutdown = shutdown.clone();
        connections.spawn(async move {
            let (mut reader, mut writer) = socket.into_split();
            if cloned_app.config.auth_required {
                match handshake(&mut reader, &mut writer, token.as_deref()).await {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(e) => {
                        eprintln!("handshake failed: {e}");
                        return;
                    }
                }
            }

            let (conn_sender, mut conn_receiver) = mpsc::channel::<WriterMessage>(100);

            let writer_shutdown = conn_shutdown.clone();
            tokio::spawn(async move {
                let mut container = Vec::new();
                loop {
                    let msg = tokio::select! {
                        biased;
                        _ = writer_shutdown.wait() => break,
                        msg = conn_receiver.recv() => match msg {
                            Some(msg) => msg,
                            None => break,
                        },
                    };

                    container.clear();
                    match msg {
                        WriterMessage::INFO { i } => {
                            container.extend_from_slice(&ResponseType::INFO.to_bytes());
                            container.extend_from_slice(&i.to_bytes());
                        }
                        WriterMessage::PING => {
                            container.extend_from_slice(&ResponseType::PONG.to_bytes());
                        }
                        WriterMessage::Msg { m } => {
                            container.extend_from_slice(&ResponseType::MSG.to_bytes());
                            container.extend_from_slice(&m.to_bytes());
                        }
                        WriterMessage::Err { err } => {
                            container.extend_from_slice(&err.to_bytes());
                        }
                    }
                    if writer.write_all(&container).await.is_err() {
                        break;
                    }
                }
            });

            let mut reader = BufReader::new(reader);
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = conn_shutdown.wait() => break,
                    frame = read_frame(&mut reader, &cloned_app.config) => frame,
                };

                match frame {
                    Ok(event) => {
                        if let Err(e) = dispatch(
                            event,
                            &conn_sender,
                            &task_sender,
                            &cloned_app.config,
                            conn_id,
                        )
                        .await
                        {
                            eprintln!("conn {conn_id} dispatch failed: {e}");
                            break;
                        }
                    }
                    Err(FrameError::Recoverable { err }) => {
                        let _ = conn_sender.send(WriterMessage::Err { err }).await;
                    }
                    Err(FrameError::Fatal { reason, code }) => {
                        if let Some(err) = code {
                            let _ = conn_sender.send(WriterMessage::Err { err }).await;
                        }
                        eprintln!("conn {conn_id} frame failed: {reason}");
                        break;
                    }
                }
            }
        });
    }

    shutdown.signal();
    while let Some(result) = connections.join_next().await {
        if let Err(e) = result {
            eprintln!("connection task panicked: {e}");
        }
    }

    drop(actor_sender);
    if let Err(e) = actor_handle.await {
        eprintln!("actor task panicked: {e}");
    }

    Ok(())
}

async fn dispatch(
    event: AppEvent,
    conn_sender: &mpsc::Sender<WriterMessage>,
    task_sender: &mpsc::Sender<Event>,
    config: &Config,
    conn_id: usize,
) -> Result<()> {
    match event {
        AppEvent::INFO => {
            conn_sender
                .send(WriterMessage::INFO { i: config.clone() })
                .await?;
        }
        AppEvent::PING => {
            conn_sender.send(WriterMessage::PING).await?;
        }
        AppEvent::SUB {
            topic,
            group,
            sub_id,
        } => {
            task_sender
                .send(Event::SUBSCRIBE {
                    topic,
                    group,
                    sender: conn_sender.clone(),
                    sub_id,
                    conn_id,
                })
                .await?;
        }
        AppEvent::PUB {
            topic,
            payload,
            timestamp,
        } => {
            task_sender
                .send(Event::PUBLISH {
                    topic,
                    payload,
                    timestamp,
                })
                .await?;
        }
        // `sub_id` is decoded but unused: a subscription is identified by
        // (topic, group, conn_id), which is already unique per connection.
        // The field is carried because SUB and UNSUB share one wire format and
        // one parser; removal deliberately does not consult it.
        AppEvent::UNSUB {
            topic,
            group,
            sub_id: _,
        } => {
            task_sender
                .send(Event::UNSUBSCRIBE {
                    topic,
                    group,
                    conn_id,
                })
                .await?;
        }
    }
    Ok(())
}

async fn read_sub_frame(
    reader: &mut BufReader<OwnedReadHalf>,
    max_control_line: usize,
) -> Result<(u8, String, String), FrameError> {
    let mut sub_id_buf = [0u8; 1];
    reader.read_exact(&mut sub_id_buf).await?;

    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let topic_len = u32::from_be_bytes(header) as usize;
    reader.read_exact(&mut header).await?;
    let group_len = u32::from_be_bytes(header) as usize;

    if topic_len > max_control_line {
        return Err(FrameError::fatal(AppError::MaxArtifactsError {
            context: "topic".to_string(),
        }));
    }
    if group_len > max_control_line {
        return Err(FrameError::fatal(AppError::MaxArtifactsError {
            context: "group".to_string(),
        }));
    }

    let mut bytes = vec![0u8; topic_len];
    reader.read_exact(&mut bytes).await?;
    let topic = String::from_utf8(bytes)?;

    validate_topic(&split(&topic), Wildcards::Allow).map_err(FrameError::fatal)?;

    bytes = vec![0u8; group_len];
    reader.read_exact(&mut bytes).await?;
    let group = String::from_utf8(bytes)?;

    Ok((sub_id_buf[0], topic, group))
}

async fn read_frame(
    reader: &mut BufReader<OwnedReadHalf>,
    config: &Config,
) -> Result<AppEvent, FrameError> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).await?;
    let cmd = buf[0];
    match cmd {
        1 => {
            return Ok(AppEvent::INFO);
        }
        2 => {
            return Ok(AppEvent::PING);
        }
        3 => {
            let (sub_id, topic, group) =
                read_sub_frame(reader, config.max_control_line as usize).await?;
            return Ok(AppEvent::SUB {
                topic,
                group,
                sub_id,
            });
        }
        4 => {
            let mut header = [0u8; 8];
            reader.read_exact(&mut header).await?;

            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&header[0..4]);
            let topic_len = u32::from_be_bytes(len_buf) as usize;
            len_buf.copy_from_slice(&header[4..8]);
            let payload_len = u32::from_be_bytes(len_buf) as usize;

            if topic_len > config.max_control_line as usize {
                return Err(FrameError::fatal(AppError::MaxArtifactsError {
                    context: "topic".to_string(),
                }));
            }
            if payload_len as u64 > config.max_payload {
                return Err(FrameError::fatal(AppError::MaxPayloadError));
            }

            let mut t_bytes = vec![0u8; topic_len];
            reader.read_exact(&mut t_bytes).await?;
            let topic = String::from_utf8(t_bytes)?;

            validate_topic(&split(&topic), Wildcards::Forbid).map_err(FrameError::fatal)?;

            t_bytes = vec![0u8; payload_len];
            reader.read_exact(&mut t_bytes).await?;

            let mut time_bytes = [0u8; 8];
            reader.read_exact(&mut time_bytes).await?;
            let timestamp = u64::from_be_bytes(time_bytes);

            return Ok(AppEvent::PUB {
                topic,
                payload: t_bytes,
                timestamp,
            });
        }
        5 => {
            let (sub_id, topic, group) =
                read_sub_frame(reader, config.max_control_line as usize).await?;
            return Ok(AppEvent::UNSUB {
                topic,
                group,
                sub_id,
            });
        }
        _ => return Err(FrameError::bare(anyhow::anyhow!("invalid command: {cmd}"))),
    }
}
