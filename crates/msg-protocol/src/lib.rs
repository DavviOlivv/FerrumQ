pub mod ferrumq {
    pub mod dataplane {
        pub mod v1 {
            tonic::include_proto!("ferrumq.dataplane.v1");
        }
    }
}

/// Returns this crate's package name.
pub fn crate_name() -> &'static str {
    "msg-protocol"
}

#[cfg(test)]
mod tests {
    use crate::ferrumq::dataplane::v1::{
        AckRequest, ConsumeRequest, PublishRequest, ferrum_q_data_plane_server::FerrumQDataPlane,
    };

    use super::crate_name;

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-protocol");
    }

    #[test]
    fn exposes_dataplane_messages() {
        let request = PublishRequest {
            topic: "orders".to_owned(),
            message_id: "message-1".to_owned(),
            key: "account-1".to_owned(),
            payload: b"{}".to_vec(),
            content_type: "application/json".to_owned(),
            r#type: "order.created".to_owned(),
            source: "/tests".to_owned(),
            subject: "subject-1".to_owned(),
            idempotency_key: "idem-1".to_owned(),
            time_unix_ms: 1,
        };

        assert_eq!(request.topic, "orders");
        assert_eq!(
            ConsumeRequest {
                topic: "orders".to_owned(),
                consumer_group: "group.1".to_owned(),
                consumer_id: "consumer-1".to_owned(),
                max_messages: 1,
                lease_ms: 1_000,
                now_unix_ms: 2,
            }
            .max_messages,
            1
        );
        assert_eq!(
            AckRequest {
                delivery_id: "delivery-1".to_owned(),
                consumer_id: "consumer-1".to_owned(),
            }
            .delivery_id,
            "delivery-1"
        );
    }

    fn assert_service_trait<T: FerrumQDataPlane>() {}

    #[allow(dead_code)]
    fn generated_server_trait_is_public<T: FerrumQDataPlane>() {
        assert_service_trait::<T>();
    }
}
