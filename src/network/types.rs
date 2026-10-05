use crate::core::types::AppError;

pub enum FrameError {
    Fatal {
        reason: anyhow::Error,
        code: Option<AppError>,
    },
    Recoverable {
        err: AppError,
    },
}

impl FrameError {
    pub fn bare(reason: impl Into<anyhow::Error>) -> Self {
        Self::Fatal {
            reason: reason.into(),
            code: None,
        }
    }

    pub fn fatal(err: AppError) -> Self {
        Self::Fatal {
            reason: anyhow::anyhow!("{err:?}"),
            code: Some(err),
        }
    }

    pub fn recoverable(err: AppError) -> Self {
        Self::Recoverable { err }
    }
}

impl From<std::io::Error> for FrameError {
    fn from(e: std::io::Error) -> Self {
        Self::bare(e)
    }
}

impl From<std::string::FromUtf8Error> for FrameError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::bare(e)
    }
}

pub enum AppEvent {
    INFO,
    PING,
    SUB {
        topic: String,
        group: String,
        sub_id: u8,
    },
    PUB {
        topic: String,
        payload: Vec<u8>,
        timestamp: u64,
    },
    UNSUB {
        topic: String,
        group: String,
        sub_id: u8,
    },
    DISCONNECT,
}

pub enum ResponseType {
    Err = 0x02,
    MSG = 0x03,
    INFO = 0x04,
    PONG = 0x05,
}

pub enum ErrorCode {
    AuthError = 0x01,
    MaxPayloadError = 0x02,
    MaxArtifactsError = 0x03,
    InvalidTopic = 0x04,
    WildcardInPublish = 0x05,
    MaxTokenLengthError = 0x06,
    AuthTimeout = 0x07,
}

impl ErrorCode {
    pub fn to_bytes(self) -> [u8; 1] {
        (self as u8).to_be_bytes()
    }
}

impl ResponseType {
    pub fn to_bytes(self) -> [u8; 1] {
        (self as u8).to_be_bytes()
    }
}

impl From<&AppError> for ErrorCode {
    fn from(err: &AppError) -> Self {
        match err {
            AppError::InvalidTopic { .. } => ErrorCode::InvalidTopic,
            AppError::WildcardInPublish { .. } => ErrorCode::WildcardInPublish,
            AppError::MaxArtifactsError { .. } => ErrorCode::MaxArtifactsError,
            AppError::MaxPayloadError => ErrorCode::MaxPayloadError,
        }
    }
}

impl AppError {
    pub fn context(&self) -> Option<&str> {
        match self {
            AppError::InvalidTopic { context }
            | AppError::WildcardInPublish { context }
            | AppError::MaxArtifactsError { context } => Some(context),
            AppError::MaxPayloadError => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ResponseType::Err.to_bytes());
        buf.extend_from_slice(&ErrorCode::from(self).to_bytes());
        if let Some(context) = self.context() {
            buf.extend_from_slice(&(context.len() as u32).to_be_bytes());
            buf.extend_from_slice(context.as_bytes());
        }
        buf
    }
}
