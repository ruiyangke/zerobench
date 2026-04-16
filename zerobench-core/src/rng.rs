//! Per-worker random number generator.
//!
//! The hot path calls this ~once per template expansion. We use
//! Xoshiro256++ (via [`rand_xoshiro`]) — a small-state, fast, non-crypto
//! PRNG. Each worker owns its own [`BenchRng`] so there is zero contention.
//!
//! Security: this RNG is **not** a CSPRNG. It's fine for `{{rand_int}}`,
//! `{{rand_hex}}`, and `{{rand_str}}` bodies (load-generation payloads),
//! but the WebSocket masking code (Task 15) must use `getrandom` directly.

use rand::{Rng, SeedableRng};
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

/// Ergonomic alias so template code reads `rng.gen_range(...)`.
///
/// This is a thin re-export of [`rand::Rng::gen_range`] kept local so
/// consumers don't need to pull in the `rand` trait in their use-clauses.
pub fn gen_range<T, R>(rng: &mut BenchRng, range: R) -> T
where
    T: rand::distributions::uniform::SampleUniform,
    R: rand::distributions::uniform::SampleRange<T>,
{
    rng.gen_range(range)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    #[test]
    fn seeded_rng_is_deterministic() {
        let mut a = from_seed(42);
        let mut b = from_seed(42);
        assert_eq!(a.next_u64(), b.next_u64());
        assert_eq!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn gen_range_in_bounds() {
        let mut rng = from_seed(1);
        for _ in 0..1000 {
            let n: i64 = gen_range(&mut rng, 1..=10);
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
