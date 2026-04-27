//! Unit tests for the SSE line framer ([`SseLineParser`]).
//!
//! The parser is the subtle part of the SSE runner — the wire parsing
//! that the v1 bench got wrong (substring-matching for chunked framing,
//! then a second substring search for line boundaries). These tests
//! exercise:
//!
//! - Single-event emission.
//! - Multi-line `data:` concatenation.
//! - CRLF tolerance.
//! - Partial feeds across chunk boundaries.
//! - Comment lines (`:heartbeat`).
//! - Non-data fields (`event:`, `id:`, `retry:`).
//! - `data: [DONE]` sentinel handling.
//! - `flush()` semantics when no trailing blank line arrives.

use zerobench_backends::sse::{SseEvent, SseLineParser};

/// Collect every event emitted while feeding `input` into a fresh
/// parser. Simpler than tracking `Vec<SseEvent>` via closure state.
fn feed_all(inputs: &[&[u8]]) -> Vec<OwnedEvent> {
    let mut p = SseLineParser::new();
    let mut out = Vec::new();
    for chunk in inputs {
        p.feed(chunk, |ev| out.push(OwnedEvent::from(ev)));
    }
    out
}

/// Feed `input` then flush, returning every event seen.
fn feed_and_flush(input: &[u8]) -> Vec<OwnedEvent> {
    let mut p = SseLineParser::new();
    let mut out = Vec::new();
    p.feed(input, |ev| out.push(OwnedEvent::from(ev)));
    p.flush(|ev| out.push(OwnedEvent::from(ev)));
    out
}

/// Owned counterpart to `SseEvent<'_>`, with lifetimes stripped so we
/// can collect events into a `Vec` and inspect them after the parser
/// borrows are released.
#[derive(Debug, PartialEq, Eq)]
enum OwnedEvent {
    Data(Vec<u8>),
    Done,
    Id(Vec<u8>),
    Ignored,
}

impl From<SseEvent<'_>> for OwnedEvent {
    fn from(ev: SseEvent<'_>) -> Self {
        match ev {
            SseEvent::Data(b) => OwnedEvent::Data(b.into_owned()),
            SseEvent::Done => OwnedEvent::Done,
            SseEvent::Id(v) => OwnedEvent::Id(v.into_owned()),
            SseEvent::Ignored => OwnedEvent::Ignored,
        }
    }
}

// ---------------------------------------------------------------------------
// Basic events
// ---------------------------------------------------------------------------

#[test]
fn single_event_emits_once_on_blank_line() {
    let out = feed_all(&[b"data: foo\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn multi_line_data_concatenates_with_newline() {
    let out = feed_all(&[b"data: a\ndata: b\n\n"]);
    // Per spec: `data: a\ndata: b\n\n` = event with payload `"a\nb"`.
    assert_eq!(out, vec![OwnedEvent::Data(b"a\nb".to_vec())]);
}

#[test]
fn crlf_line_endings_are_tolerated() {
    let out = feed_all(&[b"data: foo\r\n\r\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn mixed_crlf_and_lf_work() {
    // Real servers sometimes send `\r\n` in headers and `\n` in body.
    let out = feed_all(&[b"data: a\r\ndata: b\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"a\nb".to_vec())]);
}

#[test]
fn value_without_leading_space_is_kept_verbatim() {
    // `data:foo` → value `foo` (leading space is only stripped once).
    let out = feed_all(&[b"data:foo\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn double_leading_space_keeps_the_second_one() {
    // `data:  foo` → value ` foo` (only the first space is stripped).
    let out = feed_all(&[b"data:  foo\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b" foo".to_vec())]);
}

#[test]
fn empty_data_value_emits_empty_payload() {
    // `data:\n\n` → Data(b"")  (spec-permitted — an empty event).
    let out = feed_all(&[b"data:\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"".to_vec())]);
}

// ---------------------------------------------------------------------------
// Partial feeds
// ---------------------------------------------------------------------------

#[test]
fn split_across_two_feeds_assembles_one_event() {
    let out = feed_all(&[b"data: fo", b"o\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn split_inside_terminator_still_emits() {
    let out = feed_all(&[b"data: foo\n", b"\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn split_crlf_inside_terminator_still_emits() {
    // Boundary is right between `\r` and `\n` of the blank terminator.
    let out = feed_all(&[b"data: foo\r", b"\n\r\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn byte_at_a_time_feed_produces_one_event() {
    // Stress the per-byte state machine — if the parser forgets to
    // strip \r correctly when the \n arrives in a later call, this
    // test catches it.
    let input = b"data: hello\r\n\r\n";
    let mut p = SseLineParser::new();
    let mut out: Vec<OwnedEvent> = Vec::new();
    for &b in input {
        p.feed(&[b], |ev| out.push(OwnedEvent::from(ev)));
    }
    assert_eq!(out, vec![OwnedEvent::Data(b"hello".to_vec())]);
}

// ---------------------------------------------------------------------------
// Non-data fields
// ---------------------------------------------------------------------------

#[test]
fn comment_line_emits_ignored_on_blank_line() {
    let out = feed_all(&[b": heartbeat\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Ignored]);
}

#[test]
fn event_field_is_ignored_data_is_not() {
    // `event: foo\ndata: bar\n\n` → emit Ignored + Data? Or just Data?
    // Our policy: one event per blank-line boundary. If any non-data
    // field was seen alongside data, the data is what we count; the
    // ignored counter isn't bumped for events that also carried data.
    let out = feed_all(&[b"event: foo\ndata: bar\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Data(b"bar".to_vec())]);
}

#[test]
fn only_non_data_fields_emit_id_then_single_ignored() {
    // `id:` is surfaced immediately (for reconnect purposes);
    // `event:` and `retry:` collapse into a single trailing Ignored
    // at event dispatch.
    let out = feed_all(&[b"event: foo\nid: 42\nretry: 1000\n\n"]);
    assert_eq!(
        out,
        vec![OwnedEvent::Id(b"42".to_vec()), OwnedEvent::Ignored]
    );
}

#[test]
fn field_without_colon_is_treated_as_non_data_field() {
    // Per spec: line with no ':' is the whole line as a field name with
    // empty value. We lump this into the ignored bucket.
    let out = feed_all(&[b"bogus\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Ignored]);
}

// ---------------------------------------------------------------------------
// [DONE] sentinel
// ---------------------------------------------------------------------------

#[test]
fn done_sentinel_emits_done() {
    let out = feed_all(&[b"data: [DONE]\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Done]);
}

#[test]
fn done_replaces_concurrent_data_lines() {
    // Policy: `[DONE]` signals end-of-stream and the emit is `Done`,
    // not `Data` — any data lines in the same event are swallowed. This
    // matches OpenAI's shape in practice (they never mix).
    let out = feed_all(&[b"data: foo\ndata: [DONE]\n\n"]);
    assert_eq!(out, vec![OwnedEvent::Done]);
}

#[test]
fn normal_data_after_done_still_parses() {
    // Spec says `[DONE]` is OpenAI-only; a real SSE server might send
    // another event after. We want to keep parsing.
    let out = feed_all(&[b"data: [DONE]\n\ndata: more\n\n"]);
    assert_eq!(
        out,
        vec![OwnedEvent::Done, OwnedEvent::Data(b"more".to_vec()),]
    );
}

// ---------------------------------------------------------------------------
// Flush / partial-event handling
// ---------------------------------------------------------------------------

#[test]
fn no_blank_line_no_event_until_flush() {
    // Just a data line, no terminator — the parser holds the data.
    let out = feed_all(&[b"data: foo\n"]);
    assert_eq!(out, Vec::<OwnedEvent>::new());
}

#[test]
fn flush_emits_pending_data_event() {
    let out = feed_and_flush(b"data: foo\n");
    assert_eq!(out, vec![OwnedEvent::Data(b"foo".to_vec())]);
}

#[test]
fn flush_on_empty_input_emits_nothing() {
    let out = feed_and_flush(b"");
    assert_eq!(out, Vec::<OwnedEvent>::new());
}

#[test]
fn flush_emits_trailing_line_without_terminator() {
    // Input has no final `\n`. Flush processes the partial line.
    let out = feed_and_flush(b"data: hello");
    assert_eq!(out, vec![OwnedEvent::Data(b"hello".to_vec())]);
}

#[test]
fn multiple_events_stream() {
    let out = feed_all(&[b"data: a\n\ndata: b\n\ndata: c\n\n"]);
    assert_eq!(
        out,
        vec![
            OwnedEvent::Data(b"a".to_vec()),
            OwnedEvent::Data(b"b".to_vec()),
            OwnedEvent::Data(b"c".to_vec()),
        ]
    );
}

#[test]
fn stray_blank_lines_between_events_are_ignored() {
    // Keep-alives sometimes send just `\n` between events. We must not
    // emit an empty `Data` event for them.
    let out = feed_all(&[b"data: a\n\n\n\ndata: b\n\n"]);
    assert_eq!(
        out,
        vec![
            OwnedEvent::Data(b"a".to_vec()),
            OwnedEvent::Data(b"b".to_vec()),
        ]
    );
}

#[test]
fn consecutive_partial_writes_assemble_correctly() {
    // Simulates the TCP-boundary reality: bytes arrive in arbitrary
    // chunks. Everything should still emit exactly the 3 events.
    let chunks: &[&[u8]] = &[
        b"data: al",
        b"pha\n\nda",
        b"ta: bet",
        b"a\n\ndata: gamma\n",
        b"\n",
    ];
    let out = feed_all(chunks);
    assert_eq!(
        out,
        vec![
            OwnedEvent::Data(b"alpha".to_vec()),
            OwnedEvent::Data(b"beta".to_vec()),
            OwnedEvent::Data(b"gamma".to_vec()),
        ]
    );
}

#[test]
fn id_line_emits_id_event() {
    // Spec: `id: abc` sets the event's last-event-id. We surface it
    // as `Id(b"abc")` for the reconnect-storm caller.
    let out = feed_all(&[b"id: abc\ndata: payload\n\n"]);
    assert_eq!(
        out,
        vec![
            OwnedEvent::Id(b"abc".to_vec()),
            OwnedEvent::Data(b"payload".to_vec()),
        ]
    );
}

#[test]
fn id_split_across_chunks_is_reassembled() {
    // Regression for the B4 ad-hoc `split(b'\n')` parser that
    // dropped half of a chunk-boundary `id:` line. The parser must
    // buffer across feed() calls and emit the full id at the
    // terminating newline.
    let chunks: &[&[u8]] = &[b"id: 12", b"345\ndata: x\n\n"];
    let out = feed_all(chunks);
    assert_eq!(
        out,
        vec![
            OwnedEvent::Id(b"12345".to_vec()),
            OwnedEvent::Data(b"x".to_vec()),
        ]
    );
}

#[test]
fn id_split_across_crlf_boundary_is_stitched() {
    // A feed boundary that cuts between `\r` and `\n` on a CRLF
    // stream must still produce the correct id.
    let chunks: &[&[u8]] = &[b"id: 999\r", b"\ndata: y\n\n"];
    let out = feed_all(chunks);
    assert_eq!(
        out,
        vec![
            OwnedEvent::Id(b"999".to_vec()),
            OwnedEvent::Data(b"y".to_vec()),
        ]
    );
}
