//! Stateful Java-style backoff helpers for retry and reconnect paths.

use std::time::Duration;

use super::{Result, WireError};

/// Java clients randomize reconnect/retry backoff by 20% to avoid synchronized
/// retry storms across clients.
pub(crate) const DEFAULT_JITTER_FACTOR: f64 = 0.20;
const MIN_BACKOFF: Duration = Duration::from_millis(1);

/// Exponential backoff policy shared by reconnect, metadata refresh, and producer retry paths.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BackoffPolicy {
    initial: Duration,
    max: Duration,
    jitter_factor: f64,
}

impl BackoffPolicy {
    pub(crate) fn new(initial: Duration, max: Duration) -> Self {
        let max = max.max(MIN_BACKOFF);
        Self {
            initial: initial.max(MIN_BACKOFF).min(max),
            max,
            jitter_factor: DEFAULT_JITTER_FACTOR,
        }
    }

    pub(crate) const fn with_jitter_factor(mut self, jitter_factor: f64) -> Self {
        self.jitter_factor = if jitter_factor < 0.0 {
            0.0
        } else if jitter_factor > 1.0 {
            1.0
        } else {
            jitter_factor
        };
        self
    }
}

/// Mutable backoff state for one retry/reconnect sequence.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BackoffState {
    policy: BackoffPolicy,
    current: Duration,
}

impl BackoffState {
    pub(crate) const fn new(policy: BackoffPolicy) -> Self {
        Self {
            current: policy.initial,
            policy,
        }
    }

    pub(crate) const fn reset(&mut self) {
        self.current = self.policy.initial;
    }

    pub(crate) fn next_delay(&mut self) -> Result<Duration> {
        let mut bytes = [0_u8; 4];
        getrandom::fill(&mut bytes).map_err(|error| WireError::RandomBytes(error.to_string()))?;
        let sample = sample_from_random_bytes(bytes);
        Ok(self.next_delay_with_sample(sample))
    }

    pub(crate) fn next_delay_with_sample(&mut self, sample: f64) -> Duration {
        let base = self.current;
        self.current = base.saturating_mul(2).min(self.policy.max);
        jittered_delay(base, self.policy.jitter_factor, sample)
    }
}

fn sample_from_random_bytes(bytes: [u8; 4]) -> f64 {
    let value = u32::from_le_bytes(bytes);
    f64::from(value) / f64::from(u32::MAX)
}

fn jittered_delay(base: Duration, jitter_factor: f64, sample: f64) -> Duration {
    let sample = sample.clamp(0.0, 1.0);
    let jitter_factor = jitter_factor.clamp(0.0, 1.0);
    let low = 1.0 - jitter_factor;
    let span = jitter_factor * 2.0;
    let multiplier = low + (span * sample);
    base.mul_f64(multiplier).max(MIN_BACKOFF)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BackoffPolicy, BackoffState};

    #[test]
    fn backoff_state_starts_at_initial_and_doubles_to_max_without_jitter() {
        let policy = BackoffPolicy::new(Duration::from_millis(50), Duration::from_millis(180))
            .with_jitter_factor(0.0);
        let mut state = BackoffState::new(policy);

        assert_eq!(state.next_delay_with_sample(0.0), Duration::from_millis(50));
        assert_eq!(
            state.next_delay_with_sample(0.0),
            Duration::from_millis(100)
        );
        assert_eq!(
            state.next_delay_with_sample(0.0),
            Duration::from_millis(180)
        );
        assert_eq!(
            state.next_delay_with_sample(0.0),
            Duration::from_millis(180)
        );
    }

    #[test]
    fn backoff_state_applies_java_style_jitter_bounded_by_factor() {
        let policy = BackoffPolicy::new(Duration::from_millis(100), Duration::from_secs(1))
            .with_jitter_factor(0.2);
        let mut state = BackoffState::new(policy);

        assert_eq!(state.next_delay_with_sample(0.0), Duration::from_millis(80));
        state.reset();
        assert_eq!(
            state.next_delay_with_sample(0.5),
            Duration::from_millis(100)
        );
        state.reset();
        assert_eq!(
            state.next_delay_with_sample(1.0),
            Duration::from_millis(120)
        );
    }

    #[test]
    fn backoff_state_reset_returns_to_initial_delay() {
        let policy = BackoffPolicy::new(Duration::from_millis(25), Duration::from_millis(100))
            .with_jitter_factor(0.0);
        let mut state = BackoffState::new(policy);

        assert_eq!(state.next_delay_with_sample(0.0), Duration::from_millis(25));
        assert_eq!(state.next_delay_with_sample(0.0), Duration::from_millis(50));
        state.reset();
        assert_eq!(state.next_delay_with_sample(0.0), Duration::from_millis(25));
    }
}
