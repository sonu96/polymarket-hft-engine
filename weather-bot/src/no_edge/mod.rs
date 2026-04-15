//! Phase 3 NO-edge farmer subsystem.
//!
//! The honest-maker loop itself (ticket #7) has not landed yet — this
//! module currently hosts only the startup-bootstrap replay (`bootstrap.rs`)
//! so `main.rs` can wire it up once ticket #12 arrives.
//!
//! Per `docs/PHASE3_NO_EDGE_FARMER.md` §3.4, bootstrap is a one-shot call:
//! subscribe to WS first, fetch a one-shot Gamma `/events?active=true` page,
//! drain the WS buffer with dedup-by-slug, then hand over to the live stream.

#![allow(dead_code)]

pub mod bootstrap;
