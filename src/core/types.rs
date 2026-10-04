use crate::config::config::Config;

#[derive(Debug, Clone)]
pub struct Message {
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub id: u64,
    pub sub_id: u8,
}

impl Message {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.sub_id.to_be_bytes());
        buf.extend_from_slice(&self.id.to_be_bytes());
        buf.extend_from_slice(&self.timestamp.to_be_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}

pub enum WriterMessage {
    Msg { m: Message },
    PING,
    INFO { i: Config },
    Err { err: AppError },
}

#[derive(Debug, PartialEq)]
pub enum AppError {
    InvalidTopic { context: String },
    WildcardInPublish { context: String },
    MaxArtifactsError { context: String },
    MaxPayloadError,
}

pub enum ErrorCode {
    AuthError = 0x01,
    MaxPayloadErr = 0x02,
    MaxArtifactsErr = 0x03,
    InvalidTopic = 0x04,
    WildcardInPublish = 0x05,
    MaxTokenLengthError = 0x06,
}
