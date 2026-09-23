use buddy3d_proxy::rtsp::sdp::H264Params;
use buddy3d_proxy::rtsp::server::{SourceError, StreamSource};
use buddy3d_proxy::supervisor::{SessionEnded, State, StopHandle, StreamFactory, Supervisor};
use futures_util::FutureExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot, Notify};
use tokio::time::Instant;
use webrtc::rtp::packet::Packet;

#[derive(Default)]
struct Factory {
    calls: AtomicUsize,
    failing: AtomicBool,
    first_failure: Mutex<Option<Instant>>,
    ends: Mutex<Vec<oneshot::Sender<()>>>,
    streams: Mutex<Vec<broadcast::Sender<Packet>>>,
    stopped: Arc<Mutex<Vec<usize>>>,
    reconnect_gate: Option<Notify>,
    reconnect_started: Notify,
}

struct Stop {
    id: usize,
    stopped: Arc<Mutex<Vec<usize>>>,
}

impl Drop for Stop {
    fn drop(&mut self) {
        self.stopped.lock().unwrap().push(self.id);
    }
}

#[async_trait::async_trait]
impl StreamFactory for Factory {
    async fn connect(
        &self,
        rtp: broadcast::Sender<Packet>,
    ) -> Result<(H264Params, StopHandle, SessionEnded), SourceError> {
        let id = self.calls.fetch_add(1, Ordering::SeqCst);
        if id == 1 {
            self.reconnect_started.notify_one();
            if let Some(gate) = &self.reconnect_gate {
                gate.notified().await;
            }
        }
        if id > 0 && self.failing.load(Ordering::SeqCst) {
            self.first_failure
                .lock()
                .unwrap()
                .get_or_insert(Instant::now());
            return Err(SourceError::Unavailable("offline".into()));
        }
        let (tx, rx) = oneshot::channel();
        self.ends.lock().unwrap().push(tx);
        self.streams.lock().unwrap().push(rtp);
        Ok((
            H264Params {
                profile_level_id: "42c01e".into(),
                sprop_parameter_sets: "Z0L,aM4".into(),
                packetization_mode: 1,
                payload_type: 96,
            },
            StopHandle {
                kill: Box::new(Stop {
                    id,
                    stopped: self.stopped.clone(),
                }),
            },
            rx,
        ))
    }
}

fn supervisor(factory: Arc<Factory>) -> Arc<Supervisor> {
    Supervisor::new(
        factory,
        "cam".into(),
        "cam".into(),
        Duration::from_secs(60),
        None,
    )
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stale_watchdog_cannot_replace_new_session_after_budget_sleep() {
    let factory = Arc::new(Factory {
        failing: AtomicBool::new(true),
        ..Default::default()
    });
    let sup = supervisor(factory.clone());
    let old_viewer = sup.subscribe().await.unwrap();
    tokio::task::yield_now().await;
    factory.ends.lock().unwrap().remove(0).send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(250), async {
        while sup.snapshot().await.reconnect_tokens != 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .expect("failed reconnects did not exhaust the budget");
    assert_eq!(factory.calls.load(Ordering::SeqCst), 11);
    drop(old_viewer);
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(sup.snapshot().await.state, State::Idle);

    let next_refill = factory.first_failure.lock().unwrap().unwrap() + Duration::from_secs(300);
    // Schedule the new viewer at the refill boundary before the old watchdog.
    // advance updates the clock on its first poll, then yields; deliberately
    // do not yield this task until the new subscription has been established.
    let _ = tokio::time::advance(next_refill - Instant::now()).now_or_never();
    factory.failing.store(false, Ordering::SeqCst);
    let mut new_viewer = sup.subscribe().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        factory.calls.load(Ordering::SeqCst),
        12,
        "retired watchdog connected again"
    );
    assert!(
        !factory.stopped.lock().unwrap().contains(&11),
        "new session was stopped"
    );
    factory
        .streams
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Packet::default())
        .unwrap();
    assert!(
        new_viewer.rtp.try_recv().is_ok(),
        "new viewer lost its stream"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reconnect_finishing_after_teardown_cannot_replace_new_session() {
    let factory = Arc::new(Factory {
        reconnect_gate: Some(Notify::new()),
        ..Default::default()
    });
    let sup = supervisor(factory.clone());
    let old_viewer = sup.subscribe().await.unwrap();
    factory.ends.lock().unwrap().remove(0).send(()).unwrap();
    factory.reconnect_started.notified().await;
    drop(old_viewer);
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(sup.snapshot().await.state, State::Idle);
    let mut new_viewer = sup.subscribe().await.unwrap();
    factory.reconnect_gate.as_ref().unwrap().notify_one();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let stopped = factory.stopped.lock().unwrap().clone();
    assert!(stopped.contains(&1), "obsolete reconnect was retained");
    assert!(!stopped.contains(&2), "new session was stopped");
    factory.streams.lock().unwrap()[1]
        .send(Packet::default())
        .unwrap();
    assert!(
        new_viewer.rtp.try_recv().is_ok(),
        "new viewer lost its stream"
    );
}
