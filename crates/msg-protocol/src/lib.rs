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
    use prost::Message;

    use crate::ferrumq::dataplane::v1::{
        AckRequest, ConsumeRequest, PublishRequest, PublishResponse,
        ferrum_q_data_plane_server::FerrumQDataPlane,
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

    #[test]
    fn publish_response_wire_fixture_preserves_field_numbers() {
        let response = PublishResponse {
            topic: "o".to_owned(),
            partition: 1,
            offset: 2,
            message_id: "m".to_owned(),
            deduplicated: true,
        };

        assert_eq!(
            response.encode_to_vec(),
            vec![
                0x0a, 0x01, b'o', // topic = 1
                0x10, 0x01, // partition = 2
                0x18, 0x02, // offset = 3
                0x22, 0x01, b'm', // message_id = 4
                0x28, 0x01, // deduplicated = 5
            ]
        );
    }

    #[test]
    fn absent_deduplicated_field_decodes_to_false() {
        let bytes = [0x0a, 0x01, b'o', 0x10, 0x01, 0x18, 0x02, 0x22, 0x01, b'm'];
        let decoded = PublishResponse::decode(bytes.as_slice()).unwrap();

        assert!(!decoded.deduplicated);
        assert_eq!(decoded.message_id, "m");
    }

    #[test]
    fn unknown_publish_response_fields_are_ignored() {
        let bytes = [
            0x0a, 0x01, b'o', 0x10, 0x01, 0x18, 0x02, 0x22, 0x01, b'm', 0x98, 0x06,
            0x07, // unknown field 99, varint value 7
        ];
        let decoded = PublishResponse::decode(bytes.as_slice()).unwrap();

        assert_eq!(decoded.topic, "o");
        assert!(!decoded.deduplicated);
    }

    fn assert_service_trait<T: FerrumQDataPlane>() {}

    #[allow(dead_code)]
    fn generated_server_trait_is_public<T: FerrumQDataPlane>() {
        assert_service_trait::<T>();
    }
}
