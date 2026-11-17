use crate::driver::streams::unilocal::StreamUniLocalH3;
use crate::driver::streams::uniremote::StreamUniRemoteH3;
use crate::driver::streams::ProtoReadError;
use crate::driver::streams::ProtoWriteError;
use crate::driver::DriverError;
use crate::error::StreamWriteError;
use crate::VarInt;
use std::future::pending;
use tokio::sync::watch;
use wtransport_proto::bytes;
use wtransport_proto::error::ErrorCode;
use wtransport_proto::frame::Frame;
use wtransport_proto::frame::FrameKind;
use wtransport_proto::settings::Settings;
use wtransport_proto::stream_header::StreamKind;

pub struct LocalSettingsStream {
    stream: Option<StreamUniLocalH3>,
    settings: Settings,
}

impl LocalSettingsStream {
    pub fn empty() -> Self {
        let settings = Settings::builder()
            .qpack_max_table_capacity(VarInt::from_u32(0))
            .qpack_blocked_streams(VarInt::from_u32(0))
            .enable_connect_protocol() // TODO(biagio): it would be nice to have this only for server
            .enable_webtransport()
            .enable_h3_datagrams()
            .webtransport_max_sessions(VarInt::from_u32(1))
            .build();

        Self {
            stream: None,
            settings,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stream.is_none()
    }

    pub fn set_stream(&mut self, stream: StreamUniLocalH3) {
        assert!(matches!(stream.kind(), StreamKind::Control));
        self.stream = Some(stream);
    }

    pub async fn send_settings(&mut self) -> Result<(), DriverError> {
        match self
            .stream
            .as_mut()
            .expect("Cannot send settings on empty stream")
            .write_frame(self.settings.generate_frame())
            .await
        {
            Ok(()) => Ok(()),
            Err(ProtoWriteError::NotConnected) => Err(DriverError::NotConnected),
            Err(ProtoWriteError::Stopped) => {
                Err(DriverError::Proto(ErrorCode::ClosedCriticalStream))
            }
        }
    }

    pub async fn run(&mut self) -> DriverError {
        match self.stream.as_mut() {
            Some(stream) => match stream.stopped().await {
                StreamWriteError::NotConnected => DriverError::NotConnected,
                StreamWriteError::Closed | StreamWriteError::Stopped(_) => {
                    DriverError::Proto(ErrorCode::ClosedCriticalStream)
                }
                StreamWriteError::QuicProto => DriverError::Proto(ErrorCode::ClosedCriticalStream),
            },
            None => pending().await,
        }
    }
}

pub struct RemoteSettingsStream {
    // Frames read by a task of their own: a read dropped halfway (the driver's
    // select! drops this future at every other event) would lose bytes.
    frames: Option<tokio::sync::mpsc::Receiver<Result<Frame<'static>, DriverError>>>,
    settings: watch::Sender<Option<Settings>>,
}

impl RemoteSettingsStream {
    pub fn empty() -> Self {
        Self {
            frames: None,
            settings: watch::channel(None).0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_none()
    }

    pub fn set_stream(&mut self, stream: StreamUniRemoteH3) {
        assert!(matches!(stream.kind(), StreamKind::Control));
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut stream = stream;
            loop {
                // Ends with the driver, which drops the receiver: the stream it holds
                // would keep the connection open
                let frame = tokio::select! {
                    frame = Self::read_one(&mut stream) => frame,
                    () = tx.closed() => return,
                };
                let end = frame.is_err();
                if tx.send(frame).await.is_err() || end {
                    return;
                }
            }
        });
        self.frames = Some(rx);
    }

    pub fn subscribe(&self) -> RemoteSettingsWatcher {
        RemoteSettingsWatcher(self.settings.subscribe())
    }

    pub async fn run(&mut self) -> DriverError {
        loop {
            let frame = match self.read_frame().await {
                Ok(frame) => frame,
                Err(driver_error) => return driver_error,
            };

            if self.settings.borrow().is_none() {
                if !matches!(frame.kind(), FrameKind::Settings) {
                    return DriverError::Proto(ErrorCode::MissingSettings);
                }

                let settings = match Settings::with_frame(&frame) {
                    Ok(settings) => settings,
                    Err(error_code) => return DriverError::Proto(error_code),
                };

                self.settings.send_replace(Some(settings));
            } else if !matches!(frame.kind(), FrameKind::Exercise(_)) {
                return DriverError::Proto(ErrorCode::FrameUnexpected);
            }
        }
    }

    async fn read_frame(&mut self) -> Result<Frame<'static>, DriverError> {
        let Some(frames) = self.frames.as_mut() else {
            return pending().await;
        };
        frames
            .recv()
            .await
            .unwrap_or(Err(DriverError::NotConnected))
    }

    async fn read_one(stream: &mut StreamUniRemoteH3) -> Result<Frame<'static>, DriverError> {
        match stream.read_frame().await {
            Ok(frame) => Ok(frame),
            Err(ProtoReadError::H3(error_code)) => Err(DriverError::Proto(error_code)),
            Err(ProtoReadError::IO(io_error)) => match io_error {
                bytes::IoReadError::ImmediateFin
                | bytes::IoReadError::UnexpectedFin
                | bytes::IoReadError::Reset => {
                    Err(DriverError::Proto(ErrorCode::ClosedCriticalStream))
                }
                bytes::IoReadError::NotConnected => Err(DriverError::NotConnected),
            },
        }
    }
}

pub struct RemoteSettingsWatcher(watch::Receiver<Option<Settings>>);

impl RemoteSettingsWatcher {
    pub async fn accept_settings(&mut self) -> Option<Settings> {
        self.0.changed().await.ok()?;

        Some(
            self.0
                .borrow()
                .clone()
                .expect("On change settings must be set"),
        )
    }
}
