use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use msg_core::{
    ContentType, EventSource, EventType, MessageEnvelope, MessageId, MessagePayload,
    MessageTimestamp, Offset, PartitionId, TopicName,
};
use msg_storage::{LogConfig, PartitionLog, StorageError};
use tempfile::TempDir;

fn topic_name() -> TopicName {
    TopicName::new("orders").unwrap()
}

fn other_topic_name() -> TopicName {
    TopicName::new("payments").unwrap()
}

fn partition_id(value: u32) -> PartitionId {
    PartitionId::new(value)
}

fn config(root: &TempDir, max_segment_bytes: u64) -> LogConfig {
    LogConfig {
        root_dir: root.path().to_path_buf(),
        max_segment_bytes,
    }
}

fn envelope(id: impl AsRef<str>) -> MessageEnvelope {
    MessageEnvelope::builder(
        MessageId::new(id.as_ref()).unwrap(),
        EventSource::new("/tests").unwrap(),
        EventType::new("order.created").unwrap(),
        ContentType::new("application/json").unwrap(),
        MessageTimestamp::from_unix_millis(1_700_000_000_000),
        MessagePayload::from_bytes(br#"{"ok":true}"#.to_vec()),
    )
    .build()
}

fn open_log(root: &TempDir, max_segment_bytes: u64) -> PartitionLog {
    PartitionLog::open(
        config(root, max_segment_bytes),
        &topic_name(),
        partition_id(0),
    )
    .unwrap()
}

fn append_messages(log: &mut PartitionLog, count: u64) {
    for index in 0..count {
        assert_eq!(
            log.append(envelope(format!("message-{index}"))).unwrap(),
            Offset::new(index)
        );
    }
}

fn partition_dir(root: &Path, topic: &TopicName, partition: PartitionId) -> PathBuf {
    root.join("topics")
        .join(topic.as_str())
        .join("partitions")
        .join(partition.value().to_string())
}

fn segment_path(root: &Path, topic: &TopicName, partition: PartitionId, base: u64) -> PathBuf {
    partition_dir(root, topic, partition).join(format!("{base:020}.log"))
}

fn segment_files(root: &Path, topic: &TopicName, partition: PartitionId) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(partition_dir(root, topic, partition))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("log"))
        .collect();
    files.sort();
    files
}

fn frame_starts(path: &Path) -> Vec<u64> {
    let mut file = fs::File::open(path).unwrap();
    let file_len = file.metadata().unwrap().len();
    let mut position = 0;
    let mut starts = Vec::new();

    while position < file_len {
        starts.push(position);
        let mut length_bytes = [0_u8; 4];
        file.read_exact(&mut length_bytes).unwrap();
        let record_length = u64::from(u32::from_le_bytes(length_bytes));
        position += 8 + record_length;
        file.seek(SeekFrom::Start(position)).unwrap();
    }

    starts
}

fn flip_checksum_byte(path: &Path, frame_start: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(frame_start + 4)).unwrap();

    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(frame_start + 4)).unwrap();
    file.write_all(&byte).unwrap();
}

fn corrupt_json_payload_and_refresh_checksum(path: &Path, frame_start: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(frame_start)).unwrap();

    let mut length_bytes = [0_u8; 4];
    file.read_exact(&mut length_bytes).unwrap();
    let record_length = usize::try_from(u32::from_le_bytes(length_bytes)).unwrap();

    let mut checksum_bytes = [0_u8; 4];
    file.read_exact(&mut checksum_bytes).unwrap();

    let mut payload = vec![0_u8; record_length];
    file.read_exact(&mut payload).unwrap();
    assert_eq!(payload[0], b'{');
    payload[0] = b'[';

    let checksum = crc32fast::hash(&payload);
    file.seek(SeekFrom::Start(frame_start + 4)).unwrap();
    file.write_all(&checksum.to_le_bytes()).unwrap();
    file.seek(SeekFrom::Start(frame_start + 8)).unwrap();
    file.write_all(&payload).unwrap();
}

#[test]
fn first_append_starts_at_zero_and_offsets_are_monotonic() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1024 * 1024);

    append_messages(&mut log, 5);

    assert_eq!(log.next_offset(), Offset::new(5));
}

#[test]
fn read_from_respects_offset_limit_and_future_offsets() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1024 * 1024);
    append_messages(&mut log, 5);

    let records = log.read_from(Offset::new(2), 2).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![Offset::new(2), Offset::new(3)]
    );

    assert!(log.read_from(Offset::new(99), 10).unwrap().is_empty());
    assert!(log.read_from(Offset::new(0), 0).unwrap().is_empty());
}

#[test]
fn reopen_recovers_records_and_next_offset() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let mut reopened = open_log(&root, 1024 * 1024);
    let records = reopened.read_from(Offset::new(0), 10).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(reopened.next_offset(), Offset::new(3));
    assert_eq!(
        reopened.append(envelope("message-3")).unwrap(),
        Offset::new(3)
    );
}

#[test]
fn topic_and_partition_logs_are_isolated() {
    let root = TempDir::new().unwrap();
    let mut orders_zero =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(0)).unwrap();
    let mut orders_one =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(1)).unwrap();
    let mut payments_zero = PartitionLog::open(
        config(&root, 1024 * 1024),
        &other_topic_name(),
        partition_id(0),
    )
    .unwrap();

    assert_eq!(
        orders_zero.append(envelope("orders-0")).unwrap(),
        Offset::new(0)
    );
    assert_eq!(
        orders_one.append(envelope("orders-1")).unwrap(),
        Offset::new(0)
    );
    assert_eq!(
        payments_zero.append(envelope("payments-0")).unwrap(),
        Offset::new(0)
    );

    assert!(segment_path(root.path(), &topic_name(), partition_id(0), 0).exists());
    assert!(segment_path(root.path(), &topic_name(), partition_id(1), 0).exists());
    assert!(segment_path(root.path(), &other_topic_name(), partition_id(0), 0).exists());

    assert_eq!(orders_zero.read_from(Offset::new(0), 10).unwrap().len(), 1);
    assert_eq!(orders_one.read_from(Offset::new(0), 10).unwrap().len(), 1);
    assert_eq!(
        payments_zero.read_from(Offset::new(0), 10).unwrap().len(),
        1
    );
}

#[test]
fn rolls_segments_when_threshold_is_exceeded() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1);
    append_messages(&mut log, 3);

    let files = segment_files(root.path(), &topic_name(), partition_id(0));
    assert_eq!(files.len(), 3);
    assert_eq!(files[0].file_name().unwrap(), "00000000000000000000.log");
    assert_eq!(files[1].file_name().unwrap(), "00000000000000000001.log");
    assert_eq!(files[2].file_name().unwrap(), "00000000000000000002.log");
}

#[test]
fn reads_across_segment_boundaries() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1);
    append_messages(&mut log, 4);

    let records = log.read_from(Offset::new(1), 10).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![Offset::new(1), Offset::new(2), Offset::new(3)]
    );
}

#[test]
fn recovers_from_truncated_trailing_frame() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let original_len = fs::metadata(&path).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(original_len - 5)
        .unwrap();

    let mut recovered = open_log(&root, 1024 * 1024);
    let records = recovered.read_from(Offset::new(0), 10).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![Offset::new(0), Offset::new(1)]
    );
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(
        recovered.append(envelope("message-2b")).unwrap(),
        Offset::new(2)
    );
}

#[test]
fn recovers_from_checksum_mismatch_in_final_trailing_frame() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    flip_checksum_byte(&path, starts[2]);

    let recovered = open_log(&root, 1024 * 1024);
    let records = recovered.read_from(Offset::new(0), 10).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![Offset::new(0), Offset::new(1)]
    );
    assert_eq!(recovered.next_offset(), Offset::new(2));
}

#[test]
fn recovers_from_invalid_json_in_final_trailing_frame() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    corrupt_json_payload_and_refresh_checksum(&path, starts[2]);

    let recovered = open_log(&root, 1024 * 1024);
    let records = recovered.read_from(Offset::new(0), 10).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![Offset::new(0), Offset::new(1)]
    );
    assert_eq!(recovered.next_offset(), Offset::new(2));
}

#[test]
fn checksum_mismatch_in_middle_of_segment_errors() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    flip_checksum_byte(&path, starts[1]);

    let error =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(0)).unwrap_err();
    assert!(matches!(
        error,
        StorageError::ChecksumMismatch { position, .. } if position == starts[1]
    ));
}

#[test]
fn invalid_json_in_middle_of_segment_errors() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    corrupt_json_payload_and_refresh_checksum(&path, starts[1]);

    let error =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(0)).unwrap_err();
    assert!(matches!(error, StorageError::Serde(_)));
}

#[test]
fn invalid_config_is_rejected() {
    let root = TempDir::new().unwrap();
    let error = PartitionLog::open(config(&root, 0), &topic_name(), partition_id(0)).unwrap_err();

    assert!(matches!(error, StorageError::InvalidConfig { .. }));
}

#[test]
fn topic_name_rejects_path_traversal() {
    assert!(TopicName::new("orders/../../x").is_err());
}
