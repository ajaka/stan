use std::time::Duration;

use anyhow::Result;
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::{
    common::utils::token_len_is_valid,
    network::types::{ErrorCode, ResponseType},
};

const AUTH_TIMEOUT: Duration = Duration::from_secs(1);

enum Attempt {
    Matched,
    Wrong,
    TooLong,
}

async fn verify(reader: &mut OwnedReadHalf, token: &str) -> Result<Attempt> {
    let mut token_len_buf = [0u8; 4];
    reader.read_exact(&mut token_len_buf).await?;
    let token_len = u32::from_be_bytes(token_len_buf) as usize;
    if !token_len_is_valid(token_len) {
        return Ok(Attempt::TooLong);
    }

    let mut token_buf = vec![0u8; token_len];
    reader.read_exact(&mut token_buf).await?;
    let client = String::from_utf8(token_buf)?;
    let choice = client.as_bytes().ct_eq(token.as_bytes());
    Ok(if choice.unwrap_u8() == 1 {
        Attempt::Matched
    } else {
        Attempt::Wrong
    })
}

pub async fn handshake(
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
    token: Option<&str>,
) -> Result<bool> {
    let Some(token) = token else {
        return Ok(true);
    };

    // `timeout` yields a nested Result: the outer arm is the deadline, the
    // inner arm is I/O. Both have to be matched separately — collapsing them
    // with `?` would propagate `Elapsed` out of the function and the client
    // would never see an AuthTimeout, just a closed socket.
    let code: ErrorCode = match tokio::time::timeout(AUTH_TIMEOUT, verify(reader, token)).await {
        Ok(Ok(Attempt::Matched)) => return Ok(true),
        Ok(Ok(Attempt::Wrong)) => ErrorCode::AuthError,
        Ok(Ok(Attempt::TooLong)) => ErrorCode::MaxTokenLengthError,
        // A real read failure, not a slow client: let it propagate.
        Ok(Err(e)) => return Err(e),
        Err(_elapsed) => ErrorCode::AuthTimeout,
    };

    writer.write_all(&ResponseType::Err.to_bytes()).await?;
    writer.write_all(&code.to_bytes()).await?;
    writer.flush().await?;
    Ok(false)
}
