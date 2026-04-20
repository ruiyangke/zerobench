//! Per-worker random number generator.
//!
//! The hot path calls this ~once per template expansion. We use
//! Xoshiro256++ (via [`rand_xoshiro`]) — a small-state, fast, non-crypto
//! PRNG. Each worker owns its own [`BenchRng`] so there is zero contention.
//!
//! Security: this RNG is **not** a CSPRNG in the authentication sense.
//! It's fine for load-generation payloads (`{{rand_int}}`, `{{rand_hex}}`,
//! `{{rand_str}}`) and for WebSocket frame-mask generation (RFC 6455 §10.3),
//! where the threat model is cache-poisoning by naive intermediaries rather
//! than key-recovery — Xoshiro256++ seeded from OS entropy is immune to that
//! class of attack absent memory disclosure.

use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

/// Alias to the concrete engine used throughout zerobench. Kept as a type
/// alias so consumers code against a stable name even if we upgrade the
/// engine later.
pub type BenchRng = Xoshiro256PlusPlus;

/// Construct a worker RNG from the OS entropy source.
pub fn from_entropy() -> BenchRng {
    Xoshiro256PlusPlus::from_entropy()
}

/// Construct a worker RNG with a user-supplied 64-bit seed. Used by tests
/// that need determinism, and by the CLI's `--seed` flag (future).
pub fn from_seed(seed: u64) -> BenchRng {
    Xoshiro256PlusPlus::seed_from_u64(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, RngCore};

    #[test]
    fn seeded_rng_is_deterministic() {
        let mut a = from_seed(42);
        let mut b = from_seed(42);
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn gen_range_in_bounds() {
        // Sanity check that the underlying `rand::Rng::gen_range` works
        // as template.rs expects.
        let mut rng = from_seed(1);
        for _ in 0..1000 {
            let n: i64 = rng.gen_range(1..=10);
            assert!((1..=10).contains(&n));
        }
    }

    #[test]
    fn from_entropy_produces_distinct_streams() {
        // Extremely unlikely to produce equal u64s; seed from OS entropy.
        let a = from_entropy().next_u64();
        let b = from_entropy().next_u64();
        assert_ne!(a, b);
    }
}
