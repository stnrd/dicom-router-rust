//! Exponential backoff schedule for outbound retries.

use std::time::Duration;

use crate::config::RetryConfig;

pub struct Backoff {
  attempt:      u32,
  initial_ms:   u64,
  max_ms:       u64,
  multiplier:   f64,
  max_attempts: u32,
}

impl Backoff {
  pub fn new(cfg: &RetryConfig) -> Self {
    Self {
      attempt:      0,
      initial_ms:   cfg.initial_delay_ms,
      max_ms:       cfg.max_delay_ms,
      multiplier:   cfg.multiplier,
      max_attempts: cfg.max_attempts,
    }
  }

  /// Delay before the next attempt, or None when attempts are exhausted.
  pub fn next_delay(&mut self) -> Option<Duration> {
    if self.attempt >= self.max_attempts {
      return None;
    }
    let ms = (self.initial_ms as f64 * self.multiplier.powi(self.attempt as i32)) as u64;
    self.attempt += 1;
    Some(Duration::from_millis(ms.min(self.max_ms)))
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use super::*;

  #[test]
  fn doubles_until_cap_then_stops() {
    let cfg = crate::config::RetryConfig {
      initial_delay_ms: 1000,
      max_delay_ms:     5000,
      multiplier:       2.0,
      max_attempts:     4,
    };
    let mut b = Backoff::new(&cfg);
    assert_eq!(b.next_delay(), Some(Duration::from_millis(1000)));
    assert_eq!(b.next_delay(), Some(Duration::from_millis(2000)));
    assert_eq!(b.next_delay(), Some(Duration::from_millis(4000)));
    assert_eq!(b.next_delay(), Some(Duration::from_millis(5000)));
    assert_eq!(b.next_delay(), None);
  }
}
