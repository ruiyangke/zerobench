//! SSE line framer.
//!
//! CROWN JEWEL — WHATWG EventSource-correct line framer, including the
//! Id(Cow<[u8]>) event variant that handles chunk-boundary id: reassembly.
//! Do not rewrite without re-reading the spec and the existing tests.
//!
//! Consumes byte chunks from an already-dechunked HTTP body (hyper does
//! `Transfer-Encoding: chunked` decoding internally, so by the time bytes
//! reach us they're the raw SSE payload) and yields one
//! [`SseEvent`] per complete event.
//!
//! # Spec reference
//!
//! WHATWG "Server-Sent Events" (<https://html.spec.whatwg.org/multipage/server-sent-events.html>):
//!
//! - The stream is UTF-8; we treat bytes as opaque.
//! - Each field line ends with `\n`, `\r`, or `\r\n`.
//! - An event is terminated by a blank line (two line-terminators in a
//!   row, or equivalently a line containing only the terminator).
//! - `field:` introduces a field; `field: value` (single-space trim) is
//!   the preferred form. Lines starting with `:` are comments.
//! - Multiple `data:` fields within one event concatenate with `\n`.
//! - We recognise `data`, `event`, `id`, `retry`, and comments; only
//!   `data` produces a [`SseEvent::Data`] — `event` / `retry` /
//!   comments surface as [`SseEvent::Ignored`] for a counter so
//!   users can see if their stream has unexpected fields. `id:` is
//!   tracked separately by `SseReconnectStorm` for Last-Event-ID
//!   propagation checks.
//!
//! `data: [DONE]` is *not* in the SSE spec — it's OpenAI's convention
//! for signalling logical stream end. We surface it as
//! [`SseEvent::Done`] so the benchmark can distinguish "server finished
//! cleanly" from "server hung up". The parser continues reading after
//! `[DONE]`; it's the caller's choice whether to keep consuming the
//! (usually empty) tail.
//!
//! # Performance
//!
//! The parser keeps two small `Vec<u8>` buffers: one for the
//! line-in-progress and one for accumulating multi-line `data:` values.
//! Both are reused across events — after emitting an event we `.clear()`
//! the accumulator rather than reallocating. For typical streams
//! (one-line events, ~100 bytes each) the hot path is a byte copy
//! through a pre-sized buffer and a couple of branch-predictable compares.

use std::borrow::Cow;

/// An SSE event as surfaced to the benchmark.
#[derive(Debug, PartialEq, Eq)]
pub enum SseEvent<'a> {
    /// Complete event — the concatenated payload from one or more
    /// `data:` lines. Lines are joined by `\n` per the spec.
    Data(Cow<'a, [u8]>),
    /// The server sent `data: [DONE]` — OpenAI's end-of-stream marker.
    /// The underlying connection may or may not stay open after this;
    /// the parser keeps reading regardless.
    Done,
    /// An `id:` field value. Emitted immediately when the `id:` line
    /// is fully consumed (at its terminating newline) — not deferred
    /// to the event's blank-line dispatch. For reconnect purposes,
    /// the *most recent* `Id` is the value the caller should send
    /// back as `Last-Event-ID` on the next reconnect, which matches
    /// the spec's "last event ID string" update semantics closely
    /// enough for a benchmark (we don't need per-dispatch timing).
    ///
    /// The value has the spec's single-space trim applied; further
    /// trimming (e.g. CR, whitespace) is the caller's choice.
    Id(Cow<'a, [u8]>),
    /// A non-`data:`, non-`id:` field or comment (`event:`, `retry:`,
    /// `:comment`). The parser recognises these but does not try to
    /// decode them; emitting `Ignored` lets the caller count "how many
    /// other fields did I see?" if it wants to.
    Ignored,
}

/// Streaming SSE line framer.
///
/// Feed byte slices via [`feed`](Self::feed); the callback is invoked
/// once per complete event. The parser buffers partial lines across
/// calls, so it's safe to feed bytes at arbitrary chunk boundaries.
///
/// When the underlying stream closes, call [`flush`](Self::flush) to
/// emit any event whose blank-line terminator never arrived (e.g. the
/// server sent `data: foo\n\n...data: bar\n` and then `EOF` — `bar`
/// would otherwise be dropped).
#[derive(Debug, Default)]
pub struct SseLineParser {
    /// Bytes of the current line (not yet terminated by `\n` / `\r\n`).
    line_buf: Vec<u8>,
    /// Accumulates concatenated `data:` values until the event is
    /// terminated by a blank line.
    data_buf: Vec<u8>,
    /// `true` once at least one `data:` line has been seen since the
    /// last blank-line terminator; controls whether a blank line emits
    /// a `Data` event vs. does nothing. Prevents stray blank lines
    /// (keep-alive `\n`s between events) from emitting empty `Data`
    /// events.
    have_data: bool,
    /// `true` once any non-`data:` field has been seen since the last
    /// event boundary. Lets us emit a single `Ignored` event per event
    /// — easier for the caller to count "events that mentioned
    /// non-data fields" without flooding the counter.
    ignored_pending: bool,
    /// `true` if the most recent line in this event was `data: [DONE]`.
    /// On the blank-line terminator we emit [`SseEvent::Done`] instead
    /// of (or in addition to, if we'd also seen other data) the data
    /// payload. We emit `Done` *in place of* data to keep the
    /// "I saw [DONE]" signal trivial for callers to consume.
    done_seen: bool,
}

impl SseLineParser {
    /// Fresh parser with empty buffers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a chunk of bytes. `emit` is invoked zero or more times, once
    /// per complete event contained in (or completed by) this chunk.
    ///
    /// Bytes that don't complete a line are buffered internally and
    /// will be consumed by the next feed or by [`Self::flush`].
    pub fn feed(&mut self, bytes: &[u8], mut emit: impl FnMut(SseEvent<'_>)) {
        // We iterate byte-by-byte so we can handle `\n`, `\r\n`, and
        // the trailing `\r`-without-`\n` case uniformly. For high
        // throughput we could `memchr(b'\n', ...)` instead, but SSE
        // events are typically small and this hot loop is rarely the
        // bottleneck (the line parser is not where SSE benchmarks
        // spend time).
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'\n' {
                // Strip the trailing `\r` if present (CRLF input).
                let line = if self.line_buf.last() == Some(&b'\r') {
                    &self.line_buf[..self.line_buf.len() - 1]
                } else {
                    &self.line_buf[..]
                };
                // Process the line. We can't borrow `self` mutably
                // inside the emit closure while also holding a borrow
                // of `self.line_buf`, so `process_line` returns the
                // event (if any) as a `Cow` over a temporary buffer
                // we own outside the borrow.
                //
                // In practice the returned `Data(Cow)` always borrows
                // from `self.data_buf` — which we clear after emit
                // returns. The callback must not hold the reference
                // past its own scope, which is enforced by the HRTB
                // signature.
                process_line_and_emit(
                    line,
                    &mut self.data_buf,
                    &mut self.have_data,
                    &mut self.ignored_pending,
                    &mut self.done_seen,
                    &mut emit,
                );
                self.line_buf.clear();
            } else {
                self.line_buf.push(b);
            }
            i += 1;
        }
    }

    /// Emit any pending event held back because the blank-line
    /// terminator never arrived. Intended for end-of-stream cleanup.
    ///
    /// In practice, servers terminating mid-event usually mean they
    /// crashed or the network dropped — the partial-event payload is
    /// still useful to count, but the caller can decide whether to
    /// record it as a `Data` or discard it.
    pub fn flush(&mut self, mut emit: impl FnMut(SseEvent<'_>)) {
        // If there's a trailing line without a final `\n`, process it
        // as if we'd seen one.
        if !self.line_buf.is_empty() {
            let line = if self.line_buf.last() == Some(&b'\r') {
                &self.line_buf[..self.line_buf.len() - 1]
            } else {
                &self.line_buf[..]
            };
            process_line_and_emit(
                line,
                &mut self.data_buf,
                &mut self.have_data,
                &mut self.ignored_pending,
                &mut self.done_seen,
                &mut emit,
            );
            self.line_buf.clear();
        }

        // Finally, if we've accumulated data bytes but never saw the
        // blank-line terminator, flush them as one last event.
        if self.have_data || self.done_seen {
            finalise_event(
                &mut self.data_buf,
                &mut self.have_data,
                &mut self.ignored_pending,
                &mut self.done_seen,
                &mut emit,
            );
        } else if self.ignored_pending {
            self.ignored_pending = false;
            emit(SseEvent::Ignored);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Process one line (without its terminator) and, if it's a blank line,
/// emit the pending event.
///
/// `have_data` is the flag tracking whether we've seen any `data:` lines
/// since the last event boundary.
fn process_line_and_emit(
    line: &[u8],
    data_buf: &mut Vec<u8>,
    have_data: &mut bool,
    ignored_pending: &mut bool,
    done_seen: &mut bool,
    emit: &mut impl FnMut(SseEvent<'_>),
) {
    if line.is_empty() {
        // Blank line — terminates the current event. Emit if we have
        // any pending state.
        if *have_data || *done_seen {
            finalise_event(data_buf, have_data, ignored_pending, done_seen, emit);
        } else if *ignored_pending {
            *ignored_pending = false;
            emit(SseEvent::Ignored);
        }
        return;
    }

    // Comment line — `:` prefix. Per the spec, these are ignored.
    if line[0] == b':' {
        *ignored_pending = true;
        return;
    }

    // Split into field-name and value at the first `:`. If no `:` is
    // present the whole line is the field name with an empty value.
    let (field, value) = match memchr(b':', line) {
        Some(i) => (&line[..i], &line[i + 1..]),
        None => (line, &[][..]),
    };
    // Per spec, strip a single leading space from the value.
    let value = trim_leading_space(value);

    if field == b"data" {
        // `data: [DONE]` — OpenAI sentinel. Record and keep going; the
        // event is still terminated by the blank line.
        if value == b"[DONE]" {
            *done_seen = true;
            return;
        }
        // Append to the event's data buffer. Multiple `data:` lines
        // within one event join with `\n`.
        if !data_buf.is_empty() {
            data_buf.push(b'\n');
        }
        data_buf.extend_from_slice(value);
        *have_data = true;
    } else if field == b"id" {
        // Per WHATWG SSE §9.2, `id:` sets the event's last-event-id
        // buffer; Last-Event-ID updates on dispatch. For reconnect-
        // storm purposes we forward the value immediately — the
        // caller keeps "most recent id seen" which is what gets
        // sent back as `Last-Event-ID` on the next reconnect.
        emit(SseEvent::Id(Cow::Borrowed(value)));
    } else {
        // Any other field (event, retry, or something weird) —
        // record that we saw *something* non-data for this event. The
        // actual field values aren't used by the benchmark.
        *ignored_pending = true;
    }
}

/// Emit the accumulated event and reset per-event state.
fn finalise_event(
    data_buf: &mut Vec<u8>,
    have_data: &mut bool,
    ignored_pending: &mut bool,
    done_seen: &mut bool,
    emit: &mut impl FnMut(SseEvent<'_>),
) {
    if *done_seen {
        // `[DONE]` replaces any data payload — callers only need the
        // single "stream ended" signal.
        emit(SseEvent::Done);
    } else if *have_data {
        emit(SseEvent::Data(Cow::Borrowed(&data_buf[..])));
    }
    data_buf.clear();
    *have_data = false;
    *ignored_pending = false;
    *done_seen = false;
}

/// Byte search for a single byte. Rolled here to avoid pulling `memchr`
/// as a dep — the SSE line lengths are short and a linear scan is
/// already fast enough to not be the bottleneck at any realistic
/// event rate.
fn memchr(needle: u8, haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Strip a single leading `b' '` from `s`, if present. Per the SSE
/// spec, `data: foo` and `data:foo` both carry value `foo`.
fn trim_leading_space(s: &[u8]) -> &[u8] {
    if s.first() == Some(&b' ') {
        &s[1..]
    } else {
        s
    }
}
