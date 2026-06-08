use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use msg_core::{
    ContentType, EventSource, EventType, MessageEnvelope, MessageId, MessagePayload,
    MessageTimestamp, Offset, PartitionId, TopicName,
};
use msg_storage::{LogConfig, PartitionLog, StorageError, StoredMessageRecord};
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

fn create_partition_dir(root: &Path, topic: &TopicName, partition: PartitionId) -> PathBuf {
    let dir = partition_dir(root, topic, partition);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn assert_record_offsets(records: &[StoredMessageRecord], expected: &[u64]) {
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset.value())
            .collect::<Vec<_>>(),
        expected
    );
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

fn truncate_file(path: &Path, len: u64) {
    OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(len)
        .unwrap();
}

fn append_trailing_bytes(path: &Path, bytes: &[u8]) {
    OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
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

fn mutate_payload_and_refresh_checksum(
    path: &Path,
    frame_start: u64,
    mutate: impl FnOnce(&mut Vec<u8>),
) {
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
    mutate(&mut payload);

    let checksum = crc32fast::hash(&payload);
    file.seek(SeekFrom::Start(frame_start + 4)).unwrap();
    file.write_all(&checksum.to_le_bytes()).unwrap();
    file.seek(SeekFrom::Start(frame_start + 8)).unwrap();
    file.write_all(&payload).unwrap();
}

fn corrupt_json_payload_and_refresh_checksum(path: &Path, frame_start: u64) {
    mutate_payload_and_refresh_checksum(path, frame_start, |payload| {
        assert_eq!(payload[0], b'{');
        payload[0] = b'[';
    });
}

fn replace_payload_fragment_and_refresh_checksum(
    path: &Path,
    frame_start: u64,
    from: &[u8],
    to: &[u8],
) {
    assert_eq!(from.len(), to.len());
    mutate_payload_and_refresh_checksum(path, frame_start, |payload| {
        let start = payload
            .windows(from.len())
            .position(|window| window == from)
            .unwrap();
        payload[start..start + to.len()].copy_from_slice(to);
    });
}

#[test]
fn first_append_starts_at_zero_and_offsets_are_monotonic() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1024 * 1024);

    append_messages(&mut log, 5);

    assert_eq!(log.next_offset(), Offset::new(5));
}

#[test]
fn read_from_zero_returns_all_records_in_order() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1024 * 1024);
    append_messages(&mut log, 5);

    let records = log.read_from(Offset::new(0), 10).unwrap();
    assert_record_offsets(&records, &[0, 1, 2, 3, 4]);
}

#[test]
fn read_from_respects_offset_limit_and_future_offsets() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1024 * 1024);
    append_messages(&mut log, 5);

    let records = log.read_from(Offset::new(2), 2).unwrap();
    assert_record_offsets(&records, &[2, 3]);

    assert!(log.read_from(log.next_offset(), 10).unwrap().is_empty());
    assert!(log.read_from(Offset::new(99), 10).unwrap().is_empty());
    assert!(log.read_from(Offset::new(0), 0).unwrap().is_empty());
}

#[test]
fn failed_append_does_not_advance_next_offset_or_recovered_offset() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1);
    assert_eq!(log.append(envelope("message-0")).unwrap(), Offset::new(0));

    let blocker = segment_path(root.path(), &topic_name(), partition_id(0), 1);
    fs::create_dir(&blocker).unwrap();

    let error = log.append(envelope("message-1")).unwrap_err();
    assert!(matches!(error, StorageError::Io(_)));
    assert_eq!(log.next_offset(), Offset::new(1));

    let recovered = open_log(&root, 1);
    assert_eq!(recovered.next_offset(), Offset::new(1));
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0]);

    drop(recovered);
    fs::remove_dir(&blocker).unwrap();
    let mut recovered = open_log(&root, 1);
    assert_eq!(
        recovered.append(envelope("message-1b")).unwrap(),
        Offset::new(1)
    );
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
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
fn reopen_after_many_rolls_preserves_order_and_continues_at_next_segment() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1);
        append_messages(&mut log, 12);
    }

    let mut reopened = open_log(&root, 1);
    assert_eq!(reopened.next_offset(), Offset::new(12));
    assert_record_offsets(
        &reopened.read_from(Offset::new(0), 20).unwrap(),
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    );

    assert_eq!(
        reopened.append(envelope("message-12")).unwrap(),
        Offset::new(12)
    );
    assert!(segment_path(root.path(), &topic_name(), partition_id(0), 12).exists());
    assert_record_offsets(
        &reopened.read_from(Offset::new(10), 10).unwrap(),
        &[10, 11, 12],
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
    assert_record_offsets(&records, &[1, 2, 3]);
}

#[test]
fn rejects_unpadded_segment_file_names() {
    let root = TempDir::new().unwrap();
    let dir = create_partition_dir(root.path(), &topic_name(), partition_id(0));
    fs::write(dir.join("2.log"), []).unwrap();
    fs::write(dir.join("10.log"), []).unwrap();

    let error =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(0)).unwrap_err();
    assert!(matches!(
        error,
        StorageError::InvalidFormat { reason, .. }
            if reason == "segment file name must be a 20-digit base offset"
    ));
}

#[test]
fn rejects_out_of_sequence_segment_base_offsets() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        assert_eq!(log.append(envelope("message-0")).unwrap(), Offset::new(0));
    }
    fs::write(
        segment_path(root.path(), &topic_name(), partition_id(0), 2),
        [],
    )
    .unwrap();

    let error =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(0)).unwrap_err();
    assert!(matches!(
        error,
        StorageError::CorruptSegment { reason, .. }
            if reason == "segment base offset 2 does not match expected offset 1"
    ));
}

#[test]
fn empty_final_segment_is_accepted_and_reused() {
    let root = TempDir::new().unwrap();
    create_partition_dir(root.path(), &topic_name(), partition_id(0));
    fs::write(
        segment_path(root.path(), &topic_name(), partition_id(0), 0),
        [],
    )
    .unwrap();

    let mut log = open_log(&root, 1024 * 1024);
    assert_eq!(log.next_offset(), Offset::new(0));
    assert_eq!(log.append(envelope("message-0")).unwrap(), Offset::new(0));

    let files = segment_files(root.path(), &topic_name(), partition_id(0));
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_name().unwrap(), "00000000000000000000.log");
    assert_record_offsets(&log.read_from(Offset::new(0), 10).unwrap(), &[0]);
}

#[test]
fn empty_non_final_segment_errors() {
    let root = TempDir::new().unwrap();
    create_partition_dir(root.path(), &topic_name(), partition_id(0));
    fs::write(
        segment_path(root.path(), &topic_name(), partition_id(0), 0),
        [],
    )
    .unwrap();
    fs::write(
        segment_path(root.path(), &topic_name(), partition_id(0), 1),
        [],
    )
    .unwrap();

    let error =
        PartitionLog::open(config(&root, 1024 * 1024), &topic_name(), partition_id(0)).unwrap_err();
    assert!(matches!(
        error,
        StorageError::CorruptSegment { reason, .. } if reason == "empty non-final segment"
    ));
}

#[test]
fn missing_storage_directories_are_created() {
    let root = TempDir::new().unwrap();
    let dir = partition_dir(root.path(), &topic_name(), partition_id(0));
    assert!(!dir.exists());

    let log = open_log(&root, 1024 * 1024);

    assert_eq!(log.next_offset(), Offset::new(0));
    assert!(dir.is_dir());
}

#[test]
fn existing_storage_directories_reopen_cleanly() {
    let root = TempDir::new().unwrap();
    create_partition_dir(root.path(), &topic_name(), partition_id(0));

    let log = open_log(&root, 1024 * 1024);

    assert_eq!(log.next_offset(), Offset::new(0));
    assert!(log.read_from(Offset::new(0), 10).unwrap().is_empty());
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
fn recovers_from_truncated_final_record_length() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    truncate_file(&path, starts[2] + 2);

    let recovered = open_log(&root, 1024 * 1024);
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), starts[2]);
}

#[test]
fn recovers_from_truncated_final_checksum_header() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    truncate_file(&path, starts[2] + 6);

    let recovered = open_log(&root, 1024 * 1024);
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), starts[2]);
}

#[test]
fn recovers_from_truncated_final_payload_body() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    truncate_file(&path, file_len(&path) - 5);

    let recovered = open_log(&root, 1024 * 1024);
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), starts[2]);
}

#[test]
fn recovers_extra_trailing_bytes_after_valid_final_record() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 2);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let original_len = file_len(&path);
    append_trailing_bytes(&path, &[0xaa, 0xbb, 0xcc]);

    let recovered = open_log(&root, 1024 * 1024);
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), original_len);
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
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), starts[2]);
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
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), starts[2]);
}

#[test]
fn recovers_from_metadata_mismatch_in_final_trailing_frame() {
    let root = TempDir::new().unwrap();
    {
        let mut log = open_log(&root, 1024 * 1024);
        append_messages(&mut log, 3);
    }

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    replace_payload_fragment_and_refresh_checksum(
        &path,
        starts[2],
        br#""offset":2"#,
        br#""offset":9"#,
    );

    let recovered = open_log(&root, 1024 * 1024);
    assert_record_offsets(&recovered.read_from(Offset::new(0), 10).unwrap(), &[0, 1]);
    assert_eq!(recovered.next_offset(), Offset::new(2));
    assert_eq!(file_len(&path), starts[2]);
}

#[test]
fn checksum_mismatch_in_middle_of_segment_errors() {
    let root = TempDir::new().unwrap();
    let mut log = open_log(&root, 1024 * 1024);
    append_messages(&mut log, 3);

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    flip_checksum_byte(&path, starts[1]);

    let read_error = log.read_from(Offset::new(0), 10).unwrap_err();
    assert!(matches!(
        read_error,
        StorageError::ChecksumMismatch { position, .. } if position == starts[1]
    ));

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
    let mut log = open_log(&root, 1024 * 1024);
    append_messages(&mut log, 3);

    let path = segment_path(root.path(), &topic_name(), partition_id(0), 0);
    let starts = frame_starts(&path);
    corrupt_json_payload_and_refresh_checksum(&path, starts[1]);

    let read_error = log.read_from(Offset::new(0), 10).unwrap_err();
    assert!(matches!(read_error, StorageError::Serde(_)));

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
