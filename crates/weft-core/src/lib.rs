pub mod cancellation;
pub mod context;
pub mod error;
pub mod exec;
pub mod lane;
pub mod node;
pub mod primitive;
pub mod project;
pub mod pulse;
pub mod signal;
pub mod tag;
pub mod weft_type;

// Re-export `inventory` so the `register_signal_kind!` macro
// expanded in third-party crates (or other workspace crates) can
// reach the same crate version without adding a direct dep.
pub use inventory;

pub use cancellation::CancellationFlag;
pub use context::{ExecutionContext, Phase};
pub use error::{WeftError, WeftResult};
pub use lane::{Lane, LaneFrame};
pub use node::{
    Condition, FieldDef, FormFieldPort, FormFieldSpec, MetadataCatalog, Node, NodeCatalog,
    NodeFeatures, NodeMetadata, NodeOutput, PortDef, RuleDiagnostic, RuleSeverity,
    ValidationLevel, ValidationRule,
};
pub use primitive::{
    AwaitedEntry, AwaitedEntryKind, CostReport, ExecutionSnapshot, RootSeed, SignalAuth,
    SignalRouting, SignalSpec, SignalSurface, SuspensionInfo,
};
pub use project::{
    Edge, EdgeIndex, GroupBoundary, GroupBoundaryRole, GroupDefinition, LaneMode,
    NodeDefinition, PortDefinition, Position, ProjectDefinition,
};
pub use pulse::Pulse;
pub use weft_type::{WeftPrimitive, WeftType};

pub type Color = uuid::Uuid;
