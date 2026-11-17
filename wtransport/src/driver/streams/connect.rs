use tracing::debug;
use wtransport_proto::bytes::IoReadError;
use wtransport_proto::capsule::capsules;
use wtransport_proto::capsule::Capsule;
use wtransport_proto::capsule::{self};
use wtransport_proto::error::ErrorCode;
use wtransport_proto::frame::FrameKind;
use wtransport_proto::varint::VarInt;

use super::session::StreamSession;
use super::ProtoReadError;
use crate::driver::DriverError;
use crate::error::ApplicationClose;
use std::future::pending;

pub struct ConnectStream {
    // The stream is read by a task of its own, which ends with the stream's last word:
    // the driver's select! drops `run` at every other event, and a frame read dropped
    // halfway would lose bytes.
    done: Option<tokio::sync::oneshot::Receiver<DriverError>>,
}

impl ConnectStream {
    pub fn empty() -> Self {
        Self { done: None }
    }

    pub fn is_empty(&self) -> bool {
        self.done.is_none()
    }

    pub fn set_stream(&mut self, stream: StreamSession) {
        let (mut tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut slot = Some(stream);
            // Ends with the driver, which drops the receiver: the stream it holds
            // would keep the connection open
            let end = tokio::select! {
                end = Self::read_until_end(&mut slot) => Some(end),
                () = tx.closed() => None,
            };
            if let Some(end) = end {
                let _ = tx.send(end);
            }
        });
        self.done = Some(rx);
    }

    pub async fn run(&mut self) -> DriverError {
        match self.done.as_mut() {
            Some(done) => done.await.unwrap_or(DriverError::NotConnected),
            None => pending().await,
        }
    }

    async fn read_until_end(slot: &mut Option<StreamSession>) -> DriverError {
        let stream = match slot.as_mut() {
            Some(stream) => stream,
            None => pending().await,
        };

        loop {
            return match stream.read_frame().await {
                Ok(frame) => {
                    if !matches!(frame.kind(), FrameKind::Data) {
                        debug!("Skipping non-data frame of kind {:?}", frame.kind());
                        continue;
                    }

                    let capsule = match Capsule::with_frame(&frame) {
                        Some(capsule)
                            if matches!(
                                capsule.kind(),
                                capsule::CapsuleKind::CloseWebTransportSession
                            ) =>
                        {
                            capsule
                        }
                        // Unknown capsule, skip it
                        _ => {
                            debug!(
                                "Unknown capsule of kind {:?} of {}B",
                                frame.kind(),
                                frame.payload().len()
                            );

                            continue;
                        }
                    };

                    let close_session =
                        match capsules::CloseWebTransportSession::with_capsule(&capsule) {
                            Ok(close_session) => close_session,
                            Err(error_code) => return DriverError::Proto(error_code),
                        };

                    // reset right away to avoid receiving additional data which requires resetting with ErrorCode::Message.
                    slot.take().unwrap().reset(ErrorCode::NoError.to_code());

                    DriverError::ApplicationClosed(ApplicationClose::new(
                        close_session.error_code(),
                        close_session
                            .reason()
                            .as_bytes()
                            .to_vec()
                            .into_boxed_slice(),
                    ))
                }
                Err(ProtoReadError::H3(error_code)) => DriverError::Proto(error_code),
                Err(ProtoReadError::IO(io_error)) => match io_error {
                    // Cleanly terminating a CONNECT stream without a CLOSE_WEBTRANSPORT_SESSION
                    // capsule SHALL be semantically equivalent to terminating it with a
                    // CLOSE_WEBTRANSPORT_SESSION capsule that has an error code of 0 and an empty error string.
                    IoReadError::ImmediateFin => DriverError::ApplicationClosed(
                        ApplicationClose::new(VarInt::from_u32(0), Box::new([])),
                    ),
                    IoReadError::UnexpectedFin | IoReadError::Reset => {
                        DriverError::Proto(ErrorCode::ClosedCriticalStream)
                    }
                    IoReadError::NotConnected => DriverError::NotConnected,
                },
            };
        }
    }
}
