//! The concrete provider meters, one module per provider. Adding a provider
//! is a file here (ending in its own `register_meter!` line) plus its
//! `pub mod` line below; the registry collects the registrations at link
//! time, so there is nothing central to edit.
//! The shared toolkit these build on (the [`ProviderMeter`](crate::ProviderMeter)
//! trait, the route classification, the SSE tap helper) lives at the crate
//! root, not here: this folder is only the concrete meters.

pub mod openrouter;
