pub mod broker;
pub mod commands;
pub mod delivery;
pub mod durable;
pub mod errors;
pub(crate) mod helpers;
pub mod idempotency;
pub(crate) mod memory;
pub(crate) mod state;

pub use broker::{BrokerConfig, BrokerService};
pub use commands::{
    AckCommand, ConsumeCommand, CreateTopicCommand, DlqQuery, NackCommand, PublishCommand,
};
pub use delivery::{ConsumedMessage, DeadLetterEntry, PublishedMessage, RetrySummary};
pub use durable::{
    DurableBroker, DurableBrokerConfig, DurableBrokerError, DurableBrokerResult,
    DurableBrokerStatus,
};
pub use errors::{BrokerError, BrokerResult};

/// Returns this crate's package name.
pub fn crate_name() -> &'static str {
    "msg-broker"
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-broker");
    }
}
