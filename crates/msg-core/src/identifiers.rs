use serde::{Deserialize, Serialize};

use crate::validation::validated_string_type;

validated_string_type!(MessageId, "message_id", bounded_text);
validated_string_type!(TopicName, "topic_name", topic_name);
validated_string_type!(ConsumerGroupId, "consumer_group_id", consumer_group_id);
validated_string_type!(ConsumerId, "consumer_id", bounded_text);
validated_string_type!(SubscriptionId, "subscription_id", bounded_text);
validated_string_type!(DeliveryId, "delivery_id", bounded_text);
validated_string_type!(IdempotencyKey, "idempotency_key", bounded_text);
validated_string_type!(PartitionKey, "partition_key", bounded_text);

/// Non-negative partition identifier within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PartitionId(u32);

impl PartitionId {
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl From<PartitionId> for u32 {
    fn from(value: PartitionId) -> Self {
        value.value()
    }
}

/// Non-negative offset within a topic partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Offset(u64);

impl Offset {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl From<Offset> for u64 {
    fn from(value: Offset) -> Self {
        value.value()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn long_value() -> String {
        "a".repeat(256)
    }

    #[test]
    fn validates_topic_names() {
        for value in [
            "orders",
            "orders.created",
            "orders_created",
            "orders-created",
            "orders.v1-raw",
        ] {
            assert_eq!(TopicName::new(value).unwrap().as_str(), value);
        }

        assert_eq!(TopicName::new(" orders ").unwrap().as_str(), "orders");
    }

    #[test]
    fn rejects_invalid_topic_names() {
        for value in [
            "",
            " ",
            ".orders",
            "orders.",
            "orders..created",
            "orders created",
            "orders/created",
            "orders:created",
        ] {
            assert!(
                TopicName::new(value).is_err(),
                "expected {value:?} to be invalid"
            );
        }

        assert!(TopicName::new(long_value()).is_err());
    }

    #[test]
    fn validates_consumer_group_ids() {
        for value in ["group", "group.1", "group_1", "group-1", "."] {
            assert_eq!(ConsumerGroupId::new(value).unwrap().as_str(), value);
        }

        assert_eq!(ConsumerGroupId::new(" group ").unwrap().as_str(), "group");
    }

    #[test]
    fn rejects_invalid_consumer_group_ids() {
        for value in ["", " ", "group one", "group/one", "group:one"] {
            assert!(
                ConsumerGroupId::new(value).is_err(),
                "expected {value:?} to be invalid"
            );
        }

        assert!(ConsumerGroupId::new(long_value()).is_err());
    }

    #[test]
    fn validates_bounded_identifier_types() {
        assert_eq!(MessageId::new(" message-1 ").unwrap().as_str(), "message-1");
        assert_eq!(
            ConsumerId::new(" consumer one ").unwrap().as_str(),
            "consumer one"
        );
        assert_eq!(
            SubscriptionId::new(" subscription-1 ").unwrap().as_str(),
            "subscription-1"
        );
        assert_eq!(IdempotencyKey::new(" idem:1 ").unwrap().as_str(), "idem:1");
        assert_eq!(PartitionKey::new(" key/1 ").unwrap().as_str(), "key/1");
        assert_eq!(
            DeliveryId::new(" delivery-1 ").unwrap().as_str(),
            "delivery-1"
        );
    }

    #[test]
    fn rejects_empty_or_too_long_bounded_identifier_types() {
        macro_rules! assert_rejects_empty_or_too_long {
            ($type:ty) => {
                assert!(<$type>::new("").is_err());
                assert!(<$type>::new(" ").is_err());
                assert!(<$type>::new(long_value()).is_err());
            };
        }

        assert_rejects_empty_or_too_long!(MessageId);
        assert_rejects_empty_or_too_long!(ConsumerId);
        assert_rejects_empty_or_too_long!(SubscriptionId);
        assert_rejects_empty_or_too_long!(IdempotencyKey);
        assert_rejects_empty_or_too_long!(PartitionKey);
        assert_rejects_empty_or_too_long!(DeliveryId);
    }

    #[test]
    fn partition_ids_and_offsets_are_ordered() {
        assert!(PartitionId::new(1) > PartitionId::new(0));
        assert!(Offset::new(42) > Offset::new(7));
    }

    proptest! {
        #[test]
        fn topic_name_acceptance_matches_rules(chars in proptest::collection::vec(any::<char>(), 0..300)) {
            let input: String = chars.into_iter().collect();
            let trimmed = input.trim();
            let expected = !trimmed.is_empty()
                && trimmed.chars().count() <= 255
                && trimmed.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                && !trimmed.starts_with('.')
                && !trimmed.ends_with('.')
                && !trimmed.contains("..");

            prop_assert_eq!(TopicName::new(&input).is_ok(), expected);
        }

        #[test]
        fn offsets_preserve_u64_ordering(left in any::<u64>(), right in any::<u64>()) {
            prop_assert_eq!(Offset::new(left).cmp(&Offset::new(right)), left.cmp(&right));
        }
    }
}
