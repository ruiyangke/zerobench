//! Variable registry — slot allocation for response-extracted values.
//!
//! Each named variable gets an 8-bit slot. During execution, a
//! `ScenarioContext` carries a flat `Vec<Option<Bytes>>` indexed by
//! [`VarSlot`]; extractors write, templates read.

use serde::{Deserialize, Serialize};

/// Compile-time slot assigned to a named variable.
///
/// Slot `0` is valid. We cap the slot space at 256 per plan because realistic
/// scenarios use <10 vars; the `u8` keeps `Part::VarRef` small and
/// `ScenarioContext` cheap to initialize.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VarSlot(pub u8);

/// Name → slot mapping, built during Phase 1 and frozen into the plan.
///
/// Same name twice returns the same slot. Slot indices are sequential and
/// stable for the lifetime of the plan.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VarRegistry {
    names: Vec<String>,
}

impl VarRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self { names: Vec::new() }
    }

    /// Allocate a slot for `name`, or return the existing slot if already
    /// allocated.
    ///
    /// # Panics
    /// Panics if more than 256 distinct variables are registered (the `u8`
    /// slot space is exhausted).
    pub fn allocate(&mut self, name: impl Into<String>) -> VarSlot {
        let name = name.into();
        if let Some(idx) = self.names.iter().position(|n| n == &name) {
            return VarSlot(idx as u8);
        }
        assert!(
            self.names.len() < 256,
            "VarRegistry exceeded 256-slot capacity",
        );
        let slot = VarSlot(self.names.len() as u8);
        self.names.push(name);
        slot
    }

    /// Look up the name for a slot. Returns `None` if the slot is out of
    /// range for this registry.
    pub fn name(&self, slot: VarSlot) -> Option<&str> {
        self.names.get(slot.0 as usize).map(String::as_str)
    }

    /// Number of slots currently allocated.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// `true` if no slots are allocated.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_assigns_sequential_slots() {
        let mut r = VarRegistry::new();
        assert_eq!(r.allocate("a"), VarSlot(0));
        assert_eq!(r.allocate("b"), VarSlot(1));
        assert_eq!(r.allocate("c"), VarSlot(2));
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn allocate_returns_existing_slot_for_duplicate_name() {
        let mut r = VarRegistry::new();
        let a = r.allocate("token");
        let b = r.allocate("other");
        let a2 = r.allocate("token");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn name_round_trips() {
        let mut r = VarRegistry::new();
        let slot = r.allocate("session");
        assert_eq!(r.name(slot), Some("session"));
        assert_eq!(r.name(VarSlot(42)), None);
    }
}
