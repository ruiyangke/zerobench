//! Per-iteration execution context threaded through template expansion
//! and response processing.
//!
//! A [`ScenarioContext`] holds the small handful of pieces of state that
//! belong to *one running iteration* of a scenario:
//!
//! - Extracted variables (one slot per [`VarSlot`] declared in the plan).
//! - A worker-owned RNG for `{{rand_*}}` template parts.
//! - A per-worker monotonic counter consumed by `{{counter}}`.
//!
//! The context is borrowed by the dispatcher from the worker task for the
//! duration of each iteration, passed by `&mut` into
//! [`crate::transport::Transport::exchange`], and reused across iterations
//! via [`ScenarioContext::clear_all`] so no per-iteration allocation is
//! needed beyond the extracted values themselves.
//!
//! # Why a [`Cell<u64>`] for the counter?
//!
//! The counter lives behind a [`Cell`] (not [`std::sync::atomic::AtomicU64`])
//! because it is strictly per-worker — no cross-thread sharing. `Cell::get` /
//! `Cell::set` compile down to plain loads/stores and dodge the atomic
//! overhead. The separate `{{counter_global}}` template part reaches for a
//! `&'static AtomicU64` when process-wide uniqueness is needed.

use std::cell::Cell;

use bytes::Bytes;

use crate::rng::BenchRng;
use crate::template::ExpandCtx;
use crate::var::VarSlot;

/// Per-iteration context for template expansion and response processing.
///
/// Construct with [`ScenarioContext::new`], passing the number of variable
/// slots the plan's [`crate::var::VarRegistry`] allocated. Slot indices are
/// stable for the lifetime of the plan, so the backing `Vec` never grows.
pub struct ScenarioContext {
    /// Backing store for extracted variables, indexed by [`VarSlot`].
    /// Slots default to `None`; extractors overwrite on each iteration.
    ///
    /// Public so `build_request` can construct an [`ExpandCtx`] from
    /// individual fields without borrowing the reusable buffers.
    pub vars: Vec<Option<Bytes>>,
    /// Worker-owned random number generator. Public so templates and
    /// transports can reach it without going through a setter.
    pub rng: BenchRng,
    /// Per-worker monotonic counter — `{{counter}}` increments on each
    /// expansion. Not reset between iterations: the whole point of the
    /// counter is that it's monotonic across the entire run.
    ///
    /// Public for the same split-borrow reason as `vars`.
    pub counter: Cell<u64>,
    /// Reusable buffer for URL template expansion. Avoids a `Vec`
    /// allocation per request on the hot path.
    ///
    /// # Encapsulation
    ///
    /// Kept `pub` (with `#[doc(hidden)]`) for the legacy in-crate users;
    /// external callers should prefer [`ScenarioContext::take_url_buf`]
    /// / [`ScenarioContext::return_url_buf`] or the closure-style
    /// [`ScenarioContext::with_url_buf`] helper, which compose more
    /// cleanly with the split borrow of `rng` / `counter` / `vars`
    /// that template expansion needs.
    #[doc(hidden)]
    pub url_buf: Vec<u8>,
    /// Reusable buffer for header-name template expansion. See the
    /// `url_buf` docs for encapsulation guidance.
    #[doc(hidden)]
    pub hdr_name_buf: Vec<u8>,
    /// Reusable buffer for header-value template expansion. See the
    /// `url_buf` docs for encapsulation guidance.
    #[doc(hidden)]
    pub hdr_val_buf: Vec<u8>,
    /// Reusable buffer for body template expansion.
    pub body_buf: Vec<u8>,
}

impl ScenarioContext {
    /// Build a fresh context sized for a plan with `num_vars` variable
    /// slots. All slots start empty; the counter starts at 0.
    pub fn new(num_vars: usize, rng: BenchRng) -> Self {
        Self {
            vars: vec![None; num_vars],
            rng,
            counter: Cell::new(0),
            url_buf: Vec::with_capacity(256),
            hdr_name_buf: Vec::with_capacity(64),
            hdr_val_buf: Vec::with_capacity(256),
            body_buf: Vec::with_capacity(256),
        }
    }

    /// Read the bytes stored at `slot`, if any. `None` means the slot has
    /// never been set on this iteration (or the extractor explicitly
    /// cleared it).
    pub fn get_var(&self, slot: VarSlot) -> Option<&Bytes> {
        self.vars.get(slot.0 as usize).and_then(|o| o.as_ref())
    }

    /// Write `value` into `slot`. Out-of-range slots are silently ignored
    /// — the registry fixes the slot space at plan-compile time, so this
    /// branch represents a plan/registry mismatch that we'd rather not
    /// panic the worker over at runtime.
    pub fn set_var(&mut self, slot: VarSlot, value: Bytes) {
        if let Some(dst) = self.vars.get_mut(slot.0 as usize) {
            *dst = Some(value);
        }
    }

    /// Drop the value at `slot` (leaving `None`). Silently ignores
    /// out-of-range slots, same reasoning as [`Self::set_var`].
    pub fn clear_var(&mut self, slot: VarSlot) {
        if let Some(dst) = self.vars.get_mut(slot.0 as usize) {
            *dst = None;
        }
    }

    /// Empty every slot — typically called by the dispatcher between
    /// scenario iterations so stale extracted values don't leak across.
    pub fn clear_all(&mut self) {
        for v in &mut self.vars {
            *v = None;
        }
    }

    /// Borrow the counter cell. Exposed so [`ExpandCtx`] can thread it
    /// through without copying the whole context.
    pub fn counter_cell(&self) -> &Cell<u64> {
        &self.counter
    }

    /// Borrow the variables slice. Consumed by [`ExpandCtx::scenario_vars`]
    /// during template expansion.
    pub fn vars_slice(&self) -> &[Option<Bytes>] {
        &self.vars
    }

    /// Increment the counter and return its *post-increment* value.
    ///
    /// Useful when a transport or extractor needs a unique tag without
    /// going through template expansion. Wraps on overflow (2^64 is
    /// astronomically far off; wrapping is only defensive).
    pub fn bump_counter(&self) -> u64 {
        let next = self.counter.get().wrapping_add(1);
        self.counter.set(next);
        next
    }

    /// Build an [`ExpandCtx`] referencing this context's RNG, counter,
    /// and variables. Separated so the transport layer can expand
    /// `plan.url` / header templates / body templates with a single
    /// borrow of `&mut self`.
    ///
    /// The returned context holds a unique `&mut BenchRng` but only
    /// shared borrows of the counter and vars, which mirrors how
    /// [`crate::template::Template::expand_into`] uses them.
    pub fn expand_ctx(&mut self) -> ExpandCtx<'_> {
        ExpandCtx {
            rng: &mut self.rng,
            counter: &self.counter,
            scenario_vars: &self.vars,
        }
    }

    // ------------------------------------------------------------------
    // Reusable buffer accessors
    //
    // The URL / header-name / header-value buffers exist to avoid a
    // `Vec::with_capacity` allocation on every request. Callers that
    // need to expand a template into one of them while *also* borrowing
    // `rng` / `counter` / `vars` (the classic split-borrow dance) use
    // the `take_*` / `return_*` pair or the closure-style `with_*_buf`
    // helper below. Both APIs preserve capacity across iterations.
    // ------------------------------------------------------------------

    /// Take ownership of the URL template buffer, replacing it with an
    /// empty `Vec`. The caller expands into the returned buffer, then
    /// hands it back via [`ScenarioContext::return_url_buf`] so the
    /// capacity is retained for the next iteration.
    pub fn take_url_buf(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.url_buf)
    }

    /// Return a previously taken URL buffer to the context. Overwrites
    /// whatever is in `self.url_buf` — callers are expected to only
    /// return the buffer once per iteration.
    pub fn return_url_buf(&mut self, buf: Vec<u8>) {
        self.url_buf = buf;
    }

    /// Take both header-name and header-value buffers. Returned as a
    /// tuple so the caller can clear / expand into them while holding
    /// a split borrow of `rng` / `counter` / `vars` for template
    /// expansion. Pair with [`ScenarioContext::return_header_bufs`].
    pub fn take_header_bufs(&mut self) -> (Vec<u8>, Vec<u8>) {
        (
            std::mem::take(&mut self.hdr_name_buf),
            std::mem::take(&mut self.hdr_val_buf),
        )
    }

    /// Return previously taken header buffers.
    pub fn return_header_bufs(&mut self, name: Vec<u8>, val: Vec<u8>) {
        self.hdr_name_buf = name;
        self.hdr_val_buf = val;
    }

    /// Borrow the URL buffer and an [`ExpandCtx`] together in a
    /// closure-style API. Clears the buffer before the closure runs;
    /// the buffer's capacity is preserved across calls.
    ///
    /// Prefer this over the `take` / `return` pair when the whole
    /// expansion happens inside a single scope — the closure form
    /// statically prevents forgetting to return the buffer.
    pub fn with_url_buf<R>(
        &mut self,
        f: impl FnOnce(&mut Vec<u8>, ExpandCtx<'_>) -> R,
    ) -> R {
        let mut buf = std::mem::take(&mut self.url_buf);
        buf.clear();
        let result = {
            let ectx = ExpandCtx {
                rng: &mut self.rng,
                counter: &self.counter,
                scenario_vars: &self.vars,
            };
            f(&mut buf, ectx)
        };
        self.url_buf = buf;
        result
    }

}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::from_seed;

    #[test]
    fn set_and_get_var_round_trips() {
        let mut ctx = ScenarioContext::new(3, from_seed(1));
        ctx.set_var(VarSlot(1), Bytes::from_static(b"hello"));
        assert_eq!(
            ctx.get_var(VarSlot(1)).map(|b| b.as_ref()),
            Some(b"hello".as_ref())
        );
        // Other slots stay empty.
        assert!(ctx.get_var(VarSlot(0)).is_none());
        assert!(ctx.get_var(VarSlot(2)).is_none());
    }

    #[test]
    fn set_var_out_of_range_is_silently_ignored() {
        let mut ctx = ScenarioContext::new(1, from_seed(1));
        ctx.set_var(VarSlot(99), Bytes::from_static(b"x"));
        assert!(ctx.get_var(VarSlot(99)).is_none());
    }

    #[test]
    fn clear_var_removes_single_slot() {
        let mut ctx = ScenarioContext::new(2, from_seed(1));
        ctx.set_var(VarSlot(0), Bytes::from_static(b"a"));
        ctx.set_var(VarSlot(1), Bytes::from_static(b"b"));
        ctx.clear_var(VarSlot(0));
        assert!(ctx.get_var(VarSlot(0)).is_none());
        assert!(ctx.get_var(VarSlot(1)).is_some());
    }

    #[test]
    fn clear_all_empties_everything() {
        let mut ctx = ScenarioContext::new(3, from_seed(1));
        ctx.set_var(VarSlot(0), Bytes::from_static(b"a"));
        ctx.set_var(VarSlot(1), Bytes::from_static(b"b"));
        ctx.set_var(VarSlot(2), Bytes::from_static(b"c"));
        ctx.clear_all();
        for i in 0..3 {
            assert!(ctx.get_var(VarSlot(i)).is_none());
        }
    }

    #[test]
    fn bump_counter_increments_monotonically() {
        let ctx = ScenarioContext::new(0, from_seed(1));
        assert_eq!(ctx.bump_counter(), 1);
        assert_eq!(ctx.bump_counter(), 2);
        assert_eq!(ctx.bump_counter(), 3);
        assert_eq!(ctx.counter_cell().get(), 3);
    }

    #[test]
    fn counter_cell_starts_at_zero() {
        let ctx = ScenarioContext::new(0, from_seed(1));
        assert_eq!(ctx.counter_cell().get(), 0);
    }

    #[test]
    fn vars_slice_reflects_state() {
        let mut ctx = ScenarioContext::new(2, from_seed(1));
        ctx.set_var(VarSlot(0), Bytes::from_static(b"x"));
        let slice = ctx.vars_slice();
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0].as_ref().map(|b| b.as_ref()), Some(b"x".as_ref()));
        assert!(slice[1].is_none());
    }

    #[test]
    fn expand_ctx_uses_live_counter_and_vars() {
        let mut ctx = ScenarioContext::new(1, from_seed(1));
        ctx.set_var(VarSlot(0), Bytes::from_static(b"v"));
        let ectx = ctx.expand_ctx();
        assert_eq!(ectx.counter.get(), 0);
        assert_eq!(ectx.scenario_vars.len(), 1);
        assert_eq!(
            ectx.scenario_vars[0].as_ref().map(|b| b.as_ref()),
            Some(b"v".as_ref())
        );
    }

    #[test]
    fn take_and_return_url_buf_preserves_capacity() {
        let mut ctx = ScenarioContext::new(0, from_seed(1));
        let cap = ctx.url_buf.capacity();
        let mut buf = ctx.take_url_buf();
        assert_eq!(buf.capacity(), cap);
        // After take, the context's buffer is empty (and default-allocated).
        assert_eq!(ctx.url_buf.capacity(), 0);
        buf.extend_from_slice(b"hello");
        ctx.return_url_buf(buf);
        assert_eq!(&ctx.url_buf[..], b"hello");
        assert!(ctx.url_buf.capacity() >= cap);
    }

    #[test]
    fn take_and_return_header_bufs() {
        let mut ctx = ScenarioContext::new(0, from_seed(1));
        let (mut n, mut v) = ctx.take_header_bufs();
        n.extend_from_slice(b"x-foo");
        v.extend_from_slice(b"bar");
        ctx.return_header_bufs(n, v);
        assert_eq!(&ctx.hdr_name_buf[..], b"x-foo");
        assert_eq!(&ctx.hdr_val_buf[..], b"bar");
    }

    #[test]
    fn with_url_buf_clears_before_closure() {
        let mut ctx = ScenarioContext::new(0, from_seed(1));
        ctx.url_buf.extend_from_slice(b"stale");
        let len = ctx.with_url_buf(|buf, _ectx| {
            assert!(buf.is_empty(), "buffer should be cleared before closure");
            buf.extend_from_slice(b"ok");
            buf.len()
        });
        assert_eq!(len, 2);
        // The context's buffer now contains the closure's output.
        assert_eq!(&ctx.url_buf[..], b"ok");
    }

    #[test]
    fn with_url_buf_exposes_expand_ctx() {
        let mut ctx = ScenarioContext::new(1, from_seed(1));
        ctx.set_var(VarSlot(0), Bytes::from_static(b"val"));
        ctx.with_url_buf(|buf, ectx| {
            // scenario_vars reachable through the closure's ectx.
            assert_eq!(ectx.scenario_vars.len(), 1);
            buf.extend_from_slice(ectx.scenario_vars[0].as_ref().unwrap().as_ref());
        });
        assert_eq!(&ctx.url_buf[..], b"val");
    }
}
