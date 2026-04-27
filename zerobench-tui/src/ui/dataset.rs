//! Owned chart data wrapper — lets [`Dataset`] borrow from a
//! long-lived allocation without lifetime gymnastics.
//!
//! Inspired by scope-tui's `DataSet` pattern: the render function
//! builds an `OwnedDataset` per series, then calls `.to_dataset()` to
//! produce the ratatui widget reference right before `Chart::new`.

use ratatui::style::{Color, Style};
use ratatui::symbols::Marker;
use ratatui::widgets::{Dataset, GraphType};

/// Owns chart data so `Dataset<'a>` can borrow it without lifetime
/// gymnastics.
pub struct OwnedDataset {
    pub name: Option<String>,
    pub data: Vec<(f64, f64)>,
    pub marker: Marker,
    pub graph_type: GraphType,
    pub color: Color,
}

impl OwnedDataset {
    /// Named line series — the common case for p50/p90/rps/etc.
    pub fn line(
        name: impl Into<String>,
        data: Vec<(f64, f64)>,
        color: Color,
        marker: Marker,
    ) -> Self {
        Self {
            name: Some(name.into()),
            data,
            marker,
            graph_type: GraphType::Line,
            color,
        }
    }

    /// Unnamed reference line (target rate, zero baseline, etc.).
    pub fn reference_line(data: Vec<(f64, f64)>, color: Color, marker: Marker) -> Self {
        Self {
            name: None,
            data,
            marker,
            graph_type: GraphType::Line,
            color,
        }
    }

    /// Convert to a ratatui `Dataset` borrowing our data.
    pub fn to_dataset(&self) -> Dataset<'_> {
        let mut ds = Dataset::default()
            .marker(self.marker)
            .graph_type(self.graph_type)
            .style(Style::new().fg(self.color))
            .data(&self.data);
        if let Some(name) = &self.name {
            ds = ds.name(name.clone());
        }
        ds
    }
}
