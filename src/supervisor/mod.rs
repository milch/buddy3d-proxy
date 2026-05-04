//! Supervisor: owns the WebRTC lifecycle. First viewer triggers connect; last
//! viewer + idle timeout triggers tear-down.
//!
//! This module exposes the state machine and viewer-count plumbing. The
//! WebRTC bring-up is provided by an injected factory (`StreamFactory`) so
//! tests can substitute a stub.

pub mod webrtc_factory;

use crate::rtsp::sdp::H264Params;
use crate::rtsp::server::{SourceError, StreamSource, Subscription, ViewerEvent};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, OnceCell};
use webrtc::rtp::packet::Packet as RtpPacket;

/// Fires once when the underlying session ends on its own (signaling drop,
/// WebRTC ICE failure, peer-connection close from the camera side). Does NOT
/// fire when the session is torn down via `StopHandle::Drop`.
pub type SessionEnded = oneshot::Receiver<()>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Idle,
    Connecting,
    Streaming,
}

/// What the supervisor needs to bring up an actual stream. Tests provide
/// a mock that returns canned RTP packets; production wires through to the
/// real WebRTC + signaling stack.
#[async_trait::async_trait]
pub trait StreamFactory: Send + Sync + 'static {
    /// Bring up a new WebRTC + signaling session. Returns the negotiated H.264
    /// params and a broadcast sender that the factory keeps populating with
    /// inbound RTP packets until told to stop. The returned `StopHandle` is
    /// dropped to tear the session down.
    async fn connect(
        &self,
        rtp_tx: broadcast::Sender<RtpPacket>,
    ) -> Result<(H264Params, StopHandle, SessionEnded), SourceError>;
}

/// Drop to stop the underlying WebRTC + signaling session.
pub struct StopHandle {
    /// Held to keep the session task alive; dropping this signals shutdown.
    /// Implementations typically include a `oneshot::Sender<()>` here.
    #[allow(dead_code)]
    pub kill: Box<dyn Send + Sync>,
}

pub struct Supervisor {
    inner: Arc<SupervisorInner>,
}

struct SupervisorInner {
    factory: Arc<dyn StreamFactory>,
    camera_name: String,
    rtsp_path: String,
    idle_timeout: Duration,
    viewer_count: AtomicI32,
    /// Set when a session is up; reset when torn down.
    state: Mutex<SessionState>,
    /// Channel the RTSP server's Subscription Drop sends to.
    viewer_tx: mpsc::Sender<ViewerEvent>,
    pub wss_reconnects_total: std::sync::atomic::AtomicU64,
    pub webrtc_renegotiations_total: std::sync::atomic::AtomicU64,
    pub last_error_at: Mutex<Option<std::time::Instant>>,
    pub session_started_at: Mutex<Option<std::time::Instant>>,
}

struct SessionState {
    state: State,
    /// Broadcast channel for outbound RTP. Set when Streaming, None otherwise.
    rtp_tx: Option<broadcast::Sender<RtpPacket>>,
    /// Handle to the live session; drop to tear down.
    stop: Option<StopHandle>,
    /// H.264 params from the most recent successful negotiation. Cached so
    /// reconnects don't change the SDP we serve.
    h264: OnceCell<H264Params>,
}

impl Supervisor {
    pub fn new(
        factory: Arc<dyn StreamFactory>,
        camera_name: String,
        rtsp_path: String,
        idle_timeout: Duration,
    ) -> Arc<Self> {
        let (viewer_tx, viewer_rx) = mpsc::channel(32);
        let inner = Arc::new(SupervisorInner {
            factory,
            camera_name,
            rtsp_path,
            idle_timeout,
            viewer_count: AtomicI32::new(0),
            state: Mutex::new(SessionState {
                state: State::Idle,
                rtp_tx: None,
                stop: None,
                h264: OnceCell::new(),
            }),
            viewer_tx,
            wss_reconnects_total: std::sync::atomic::AtomicU64::new(0),
            webrtc_renegotiations_total: std::sync::atomic::AtomicU64::new(0),
            last_error_at: Mutex::new(None),
            session_started_at: Mutex::new(None),
        });

        // Spawn the viewer-event loop (handles idle timer + tear-down).
        let inner_for_loop = inner.clone();
        tokio::spawn(viewer_event_loop(inner_for_loop, viewer_rx));

        Arc::new(Self { inner })
    }

    /// Cheap, lock-free read of operational counters for the metrics emitter.
    /// Locks the small `last_error_at` / `session_started_at` mutexes briefly.
    pub async fn snapshot(&self) -> SupervisorSnapshot {
        let state = self.inner.state.lock().await;
        let session_started_at = *self.inner.session_started_at.lock().await;
        let last_error_at = *self.inner.last_error_at.lock().await;
        SupervisorSnapshot {
            state: state.state,
            viewers: self.inner.viewer_count.load(Ordering::SeqCst).max(0) as u32,
            wss_reconnects_total: self.inner.wss_reconnects_total.load(Ordering::SeqCst),
            webrtc_renegotiations_total: self.inner.webrtc_renegotiations_total.load(Ordering::SeqCst),
            session_uptime_secs: session_started_at.map(|t| t.elapsed().as_secs()),
            last_error_age_secs: last_error_at.map(|t| t.elapsed().as_secs()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SupervisorSnapshot {
    pub state: State,
    pub viewers: u32,
    pub wss_reconnects_total: u64,
    pub webrtc_renegotiations_total: u64,
    pub session_uptime_secs: Option<u64>,
    pub last_error_age_secs: Option<u64>,
}

#[async_trait::async_trait]
impl StreamSource for Supervisor {
    async fn subscribe(&self) -> Result<Subscription, SourceError> {
        // Increment optimistically; we'll undo on failure.
        self.inner.viewer_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.inner.viewer_tx.send(ViewerEvent::Attached).await;

        // Acquire (or reacquire) the session.
        {
            let mut state = self.inner.state.lock().await;
            if matches!(state.state, State::Idle) {
                state.state = State::Connecting;
                // Drop the lock while we connect — we don't want to block other
                // viewers' subscribe() calls if they race.
                drop(state);

                let (h264_params, stop, broadcast_tx, ended) = match connect_session(&self.inner).await {
                    Ok(t) => t,
                    Err(e) => {
                        // Roll back the viewer count.
                        self.inner.viewer_count.fetch_sub(1, Ordering::SeqCst);
                        let _ = self.inner.viewer_tx.send(ViewerEvent::Detached).await;
                        *self.inner.last_error_at.lock().await = Some(std::time::Instant::now());
                        return Err(e);
                    }
                };
                *self.inner.session_started_at.lock().await = Some(std::time::Instant::now());

                let mut state = self.inner.state.lock().await;
                state.state = State::Streaming;
                state.rtp_tx = Some(broadcast_tx.clone());
                state.stop = Some(stop);
                let _ = state.h264.set(h264_params);
                drop(state);

                // Spawn the watchdog that handles spontaneous session death.
                let watchdog_inner = self.inner.clone();
                let watchdog_tx = broadcast_tx.clone();
                tokio::spawn(reconnect_watchdog(watchdog_inner, ended, watchdog_tx));
            }
            // state guard dropped here whether or not we connected.
        }

        // At this point a session exists; subscribe to it.
        let state = self.inner.state.lock().await;
        let h264 = state
            .h264
            .get()
            .cloned()
            .expect("h264 params set after connect");
        let rtp_tx = state.rtp_tx.as_ref().expect("rtp_tx set in Streaming");
        let rtp_rx = rtp_tx.subscribe();
        Ok(Subscription {
            h264,
            rtp: rtp_rx,
            on_drop: self.inner.viewer_tx.clone(),
        })
    }

    fn camera_name(&self) -> &str {
        &self.inner.camera_name
    }
    fn rtsp_path(&self) -> &str {
        &self.inner.rtsp_path
    }
}

async fn connect_session(
    inner: &Arc<SupervisorInner>,
) -> Result<(H264Params, StopHandle, broadcast::Sender<RtpPacket>, SessionEnded), SourceError> {
    let (broadcast_tx, _) = broadcast::channel::<RtpPacket>(256);
    let (h264, stop, ended) = inner.factory.connect(broadcast_tx.clone()).await?;
    Ok((h264, stop, broadcast_tx, ended))
}

async fn reconnect_watchdog(
    inner: Arc<SupervisorInner>,
    first_ended: SessionEnded,
    broadcast_tx: broadcast::Sender<RtpPacket>,
) {
    let mut backoff = crate::backoff::ExpBackoff::new();
    let mut ended = first_ended;

    'outer: loop {
        // Block until the current session ends. If recv errors, the sender
        // was dropped — StopHandle::Drop fired (orderly tear-down), so exit.
        if ended.await.is_err() {
            return;
        }

        // No viewers? Don't reconnect — supervisor will go Idle on its own.
        if inner.viewer_count.load(Ordering::SeqCst) <= 0 {
            return;
        }

        // Retry loop: keep trying until success OR all viewers leave.
        loop {
            let delay = backoff.next_delay();
            tracing::warn!(
                attempt = backoff.attempt(),
                delay_ms = delay.as_millis() as u64,
                "session ended; backing off before reconnect"
            );
            tokio::time::sleep(delay).await;

            // If teardown happened during the sleep (viewers left and idle timer fired),
            // state will be Idle. A fresh subscribe() will have spawned its own watchdog;
            // this stale one should exit cleanly.
            {
                let state = inner.state.lock().await;
                if !matches!(state.state, State::Streaming | State::Connecting) {
                    return;
                }
            }

            if inner.viewer_count.load(Ordering::SeqCst) <= 0 {
                return;
            }

            match inner.factory.connect(broadcast_tx.clone()).await {
                Ok((_h264, stop, new_ended)) => {
                    let mut state = inner.state.lock().await;
                    // Defensive: if teardown ran between our sleep and our connect,
                    // a fresh subscribe() will own the new session. Drop ours and exit.
                    if matches!(state.state, State::Idle) {
                        return;
                    }
                    state.stop = Some(stop);
                    state.state = State::Streaming;
                    drop(state);
                    *inner.session_started_at.lock().await = Some(std::time::Instant::now());
                    inner.wss_reconnects_total.fetch_add(1, Ordering::SeqCst);
                    backoff.reset();
                    ended = new_ended;
                    continue 'outer;
                }
                Err(e) => {
                    *inner.last_error_at.lock().await = Some(std::time::Instant::now());
                    tracing::warn!(error = %e, "reconnect attempt failed; will retry");
                    // Loop again — next iteration applies the next backoff delay.
                }
            }
        }
    }
}

async fn viewer_event_loop(inner: Arc<SupervisorInner>, mut rx: mpsc::Receiver<ViewerEvent>) {
    let mut idle_timer: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(ev) = rx.recv().await {
        let count = match ev {
            ViewerEvent::Attached => {
                if let Some(h) = idle_timer.take() {
                    h.abort();
                }
                inner.viewer_count.load(Ordering::SeqCst)
            }
            ViewerEvent::Detached => {
                let after = inner.viewer_count.fetch_sub(1, Ordering::SeqCst) - 1;
                tracing::debug!(viewers = after, "viewer detached");
                if after <= 0 {
                    // Start the idle timer.
                    let inner_for_timer = inner.clone();
                    let timeout = inner.idle_timeout;
                    idle_timer = Some(tokio::spawn(async move {
                        tokio::time::sleep(timeout).await;
                        teardown_session(&inner_for_timer).await;
                    }));
                }
                after
            }
        };
        tracing::debug!(viewers = count, "viewer event");
    }
}

async fn teardown_session(inner: &Arc<SupervisorInner>) {
    let mut state = inner.state.lock().await;
    if inner.viewer_count.load(Ordering::SeqCst) > 0 {
        return; // Someone reconnected; cancel the tear-down.
    }
    if matches!(state.state, State::Streaming | State::Connecting) {
        tracing::info!("idle timeout reached; tearing down webrtc session");
        state.stop = None; // Drop = stop signal.
        state.rtp_tx = None;
        state.state = State::Idle;
        // Reset the h264 cell so next connect can set fresh params.
        state.h264 = OnceCell::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A factory that records how many times it was asked to connect, and
    /// returns canned H.264 params + a stub stop handle.
    struct StubFactory {
        connects: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl StreamFactory for StubFactory {
        async fn connect(
            &self,
            _rtp_tx: broadcast::Sender<RtpPacket>,
        ) -> Result<(H264Params, StopHandle, SessionEnded), SourceError> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            let (_tx, rx) = oneshot::channel();
            Ok((
                H264Params {
                    profile_level_id: "42c01e".into(),
                    sprop_parameter_sets: "Z0L,aM4".into(),
                    packetization_mode: 1,
                    payload_type: 96,
                },
                StopHandle {
                    kill: Box::new(()),
                },
                rx,
            ))
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn first_subscribe_brings_up_session() {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(StubFactory {
            connects: connects.clone(),
        });
        let sup = Supervisor::new(
            factory,
            "Cam".into(),
            "cam".into(),
            Duration::from_secs(60),
        );
        let _sub = sup.subscribe().await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn second_subscribe_reuses_session() {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(StubFactory {
            connects: connects.clone(),
        });
        let sup = Supervisor::new(
            factory,
            "Cam".into(),
            "cam".into(),
            Duration::from_secs(60),
        );
        let _sub1 = sup.subscribe().await.unwrap();
        let _sub2 = sup.subscribe().await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn idle_timer_tears_down_after_last_viewer_drops() {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(StubFactory {
            connects: connects.clone(),
        });
        let sup = Supervisor::new(
            factory,
            "Cam".into(),
            "cam".into(),
            Duration::from_secs(5),
        );
        let sub = sup.subscribe().await.unwrap();
        drop(sub);
        // Allow detach event to be processed.
        tokio::task::yield_now().await;
        // Advance past idle timeout.
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        // Now subscribing again should bring up a NEW session.
        let _sub2 = sup.subscribe().await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }

    struct EndingFactory {
        connects: Arc<AtomicUsize>,
        /// Holds the FIRST connect's ended-tx so the test can fire it.
        fire_after: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    }

    #[async_trait::async_trait]
    impl StreamFactory for EndingFactory {
        async fn connect(
            &self,
            _rtp_tx: broadcast::Sender<RtpPacket>,
        ) -> Result<(H264Params, StopHandle, SessionEnded), SourceError> {
            let n = self.connects.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = oneshot::channel();
            // Only the FIRST connect's ended-tx is published; subsequent connects
            // return a dangling rx so the watchdog blocks forever.
            if n == 0 {
                *self.fire_after.lock().await = Some(tx);
            }
            Ok((
                H264Params {
                    profile_level_id: "42c01e".into(),
                    sprop_parameter_sets: "Z0L,aM4".into(),
                    packetization_mode: 1,
                    payload_type: 96,
                },
                StopHandle {
                    kill: Box::new(()),
                },
                rx,
            ))
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn watchdog_reconnects_after_session_ends() {
        let connects = Arc::new(AtomicUsize::new(0));
        let fire = Arc::new(Mutex::new(None));
        let factory = Arc::new(EndingFactory {
            connects: connects.clone(),
            fire_after: fire.clone(),
        });
        let sup = Supervisor::new(factory, "Cam".into(), "cam".into(), Duration::from_secs(60));
        let _sub = sup.subscribe().await.unwrap();
        // Let the watchdog task be polled and reach `ended.await`.
        tokio::task::yield_now().await;
        // Trigger spontaneous session end.
        fire.lock().await.take().unwrap().send(()).unwrap();
        // Let the watchdog see the ended-channel and reach `sleep().await`.
        tokio::task::yield_now().await;
        // Backoff first delay is ~1s with jitter; advance well past the cap.
        tokio::time::advance(Duration::from_secs(2)).await;
        // Give the watchdog cycles to wake from sleep and call connect().
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn reattach_within_idle_window_keeps_session() {
        let connects = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(StubFactory {
            connects: connects.clone(),
        });
        let sup = Supervisor::new(
            factory,
            "Cam".into(),
            "cam".into(),
            Duration::from_secs(60),
        );
        let sub = sup.subscribe().await.unwrap();
        drop(sub);
        tokio::task::yield_now().await;
        // Advance only 10 seconds (under the 60s timeout).
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        // Reattach should reuse the existing session.
        let _sub2 = sup.subscribe().await.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }
}
