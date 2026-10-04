use std::sync::Arc;

use tokio::sync::watch;

/// What the server is currently doing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    Running,
    ShuttingDown,
}

/// A one-way latch that every task can watch.
///
/// Backed by `watch`, which stores the current value rather than signalling an
/// edge. That distinction is the whole point: a task that calls [`Shutdown::wait`]
/// after the signal has already fired returns immediately, so there is no window
/// in which a signal can be missed.
#[derive(Clone)]
pub struct Shutdown {
    tx: Arc<watch::Sender<State>>,
}

impl Shutdown {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(State::Running);
        Self { tx: Arc::new(tx) }
    }

    /// Flips the latch. Every clone sees it; idempotent.
    ///
    /// `send_replace` rather than `send`: `send` is a no-op when the channel
    /// has no receivers, which is exactly the case here because `wait` creates
    /// its own receiver on demand. `send_replace` updates the value regardless.
    pub fn signal(&self) {
        self.tx.send_replace(State::ShuttingDown);
    }

    /// Resolves once shutdown has been signalled, however late the caller is.
    pub async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        // `wait_for` re-checks before it parks, so a signal raised before this
        // line still returns rather than hanging.
        let _ = rx.wait_for(|state| *state == State::ShuttingDown).await;
    }

    /// Non-blocking check, for places that would rather branch than await.
    pub fn is_shutting_down(&self) -> bool {
        *self.tx.borrow() == State::ShuttingDown
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn a_fresh_latch_is_running() {
        let shutdown = Shutdown::new();
        assert!(!shutdown.is_shutting_down());
    }

    #[tokio::test]
    async fn signal_is_visible_immediately_after() {
        let shutdown = Shutdown::new();
        shutdown.signal();
        assert!(shutdown.is_shutting_down());
    }

    #[tokio::test]
    async fn wait_resolves_after_a_signal_from_another_task() {
        let shutdown = Shutdown::new();
        let handle = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                shutdown.signal();
            }
        });

        shutdown.wait().await;
        handle.await.unwrap();
    }

    /// The race this type exists for: the waiter registers *after* the signal.
    /// With an edge-triggered notify this would hang.
    #[tokio::test]
    async fn wait_returns_even_if_signalled_before_it_was_called() {
        let shutdown = Shutdown::new();
        shutdown.signal();

        tokio::time::timeout(Duration::from_millis(100), shutdown.wait())
            .await
            .expect("wait must not hang after an earlier signal");
    }

    #[tokio::test]
    async fn every_clone_observes_the_same_signal() {
        let shutdown = Shutdown::new();
        let clone = shutdown.clone();
        let other = shutdown.clone();

        clone.signal();

        assert!(other.is_shutting_down());
        tokio::time::timeout(Duration::from_millis(100), other.wait())
            .await
            .expect("clones share one state");
    }

    #[tokio::test]
    async fn signalling_twice_is_harmless() {
        let shutdown = Shutdown::new();
        shutdown.signal();
        shutdown.signal();

        assert!(shutdown.is_shutting_down());
        tokio::time::timeout(Duration::from_millis(100), shutdown.wait())
            .await
            .expect("second signal is a no-op");
    }

    #[tokio::test]
    async fn wait_stays_pending_while_running() {
        let shutdown = Shutdown::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), shutdown.wait())
                .await
                .is_err(),
            "wait must not resolve before a signal"
        );
    }
}
