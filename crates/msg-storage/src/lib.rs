use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use msg_core::{MessageEnvelope, Offset, PartitionId, TopicName};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const FORMAT_VERSION: u8 = 1;
const FRAME_HEADER_BYTES: u64 = 8;
const SEGMENT_FILE_EXTENSION: &str = "log";
const SEGMENT_FILE_NAME_WIDTH: usize = 20;

/// Storage crate result type.
pub type StorageResult<T> = Result<T, StorageError>;

/// Local append-only log configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    /// Root directory under which topic and partition log files are stored.
    pub root_dir: PathBuf,
    /// Segment roll threshold. A single record larger than this value is still
    /// written to an empty segment.
    pub max_segment_bytes: u64,
}

/// Durable record returned from a partition log read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessageRecord {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: Offset,
    pub envelope: MessageEnvelope,
}

/// Storage failures surfaced by the append-only log.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid storage config: {reason}")]
    InvalidConfig { reason: String },

    #[error("invalid storage format in {path}: {reason}")]
    InvalidFormat { path: PathBuf, reason: String },

    #[error(
        "checksum mismatch in {path} at byte {position}: expected {expected:#010x}, actual {actual:#010x}"
    )]
    ChecksumMismatch {
        path: PathBuf,
        position: u64,
        expected: u32,
        actual: u32,
    },

    #[error("corrupt segment {path}: {reason}")]
    CorruptSegment { path: PathBuf, reason: String },

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Synchronous segment-backed append-only log for one topic partition.
///
/// Offsets are zero-based, monotonic, and gapless for successful appends within
/// a partition. Segment files are named by fixed-width 20-digit base offsets;
/// unpadded `.log` file names are rejected during recovery.
#[derive(Debug, Clone)]
pub struct PartitionLog {
    config: LogConfig,
    topic: TopicName,
    partition: PartitionId,
    partition_dir: PathBuf,
    segments: Vec<SegmentMetadata>,
    next_offset: Offset,
}

#[derive(Debug, Clone)]
struct SegmentMetadata {
    base_offset: Offset,
    next_offset: Offset,
    path: PathBuf,
    size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedRecord {
    format_version: u8,
    topic: TopicName,
    partition: PartitionId,
    offset: Offset,
    envelope: MessageEnvelope,
}

#[derive(Debug)]
struct SegmentScan {
    records: Vec<StoredMessageRecord>,
    next_offset: Offset,
    size_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct ScanMode {
    repair_final_trailing_record: bool,
}

impl ScanMode {
    const STRICT: Self = Self {
        repair_final_trailing_record: false,
    };

    const RECOVER_FINAL: Self = Self {
        repair_final_trailing_record: true,
    };
}

impl PartitionLog {
    /// Opens or creates the local log for a validated topic and partition.
    ///
    /// The on-disk layout is:
    /// `<root>/topics/<topic>/partitions/<partition-id>/<20-digit-base-offset>.log`.
    ///
    /// Recovery scans segment files in numeric base-offset order. Only trailing
    /// damage in the final segment is auto-repaired, by truncating to the start
    /// of the damaged record. Corruption in a middle record or any non-final
    /// segment returns a typed storage error.
    pub fn open(
        config: LogConfig,
        topic: &TopicName,
        partition: PartitionId,
    ) -> StorageResult<Self> {
        validate_config(&config)?;

        let partition_dir = partition_dir(&config.root_dir, topic, partition);
        fs::create_dir_all(&partition_dir)?;

        let mut log = Self {
            config,
            topic: topic.clone(),
            partition,
            partition_dir,
            segments: Vec::new(),
            next_offset: Offset::new(0),
        };
        log.recover()?;
        Ok(log)
    }

    /// Appends one envelope and returns its assigned partition offset.
    ///
    /// The first successful append returns offset `0`. Successful appends are
    /// monotonic and gapless per partition. If an append returns an error,
    /// `next_offset` is not advanced; write or flush failures are rolled back to
    /// the segment length observed before the append when the filesystem allows
    /// truncation. The current write path calls [`Write::flush`]; explicit fsync
    /// policy tuning is deferred.
    pub fn append(&mut self, envelope: MessageEnvelope) -> StorageResult<Offset> {
        let offset = self.next_offset;
        let next_offset = increment_offset(offset, &self.partition_dir)?;
        let persisted = PersistedRecord {
            format_version: FORMAT_VERSION,
            topic: self.topic.clone(),
            partition: self.partition,
            offset,
            envelope,
        };
        let payload = serde_json::to_vec(&persisted)?;
        let frame = encode_frame(&payload, &self.partition_dir)?;
        let frame_size = u64::try_from(frame.len()).map_err(|_| StorageError::InvalidFormat {
            path: self.partition_dir.clone(),
            reason: "frame length does not fit in u64".to_owned(),
        })?;

        self.roll_if_needed(frame_size)?;
        let segment_index = self.ensure_active_segment()?;
        let segment_path = self.segments[segment_index].path.clone();
        let rollback_len = self.segments[segment_index].size_bytes;
        let new_segment_size =
            rollback_len
                .checked_add(frame_size)
                .ok_or_else(|| StorageError::InvalidFormat {
                    path: segment_path.clone(),
                    reason: "segment size overflow".to_owned(),
                })?;
        append_frame_with_rollback(&segment_path, &frame, rollback_len)?;

        let segment = &mut self.segments[segment_index];
        segment.size_bytes = new_segment_size;
        segment.next_offset = next_offset;
        self.next_offset = next_offset;

        Ok(offset)
    }

    /// Reads up to `limit` records starting at `offset`.
    ///
    /// Reads past the current end, reads from the exact next offset, and reads
    /// with `limit == 0` return `Ok(Vec::new())`.
    pub fn read_from(
        &self,
        offset: Offset,
        limit: usize,
    ) -> StorageResult<Vec<StoredMessageRecord>> {
        if limit == 0 || offset >= self.next_offset {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for segment in &self.segments {
            if records.len() >= limit {
                break;
            }

            if segment.next_offset <= offset {
                continue;
            }

            let scan = scan_segment(
                &segment.path,
                &self.topic,
                self.partition,
                segment.base_offset,
                ScanMode::STRICT,
            )?;

            for record in scan.records {
                if record.offset >= offset {
                    records.push(record);
                    if records.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(records)
    }

    /// Returns the offset that will be assigned to the next successful append.
    #[must_use]
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    fn recover(&mut self) -> StorageResult<()> {
        let segment_files = discover_segment_files(&self.partition_dir)?;
        let mut expected_offset = Offset::new(0);
        let final_index = segment_files.len().checked_sub(1);

        for (index, segment_file) in segment_files.into_iter().enumerate() {
            if segment_file.base_offset != expected_offset {
                return Err(StorageError::CorruptSegment {
                    path: segment_file.path,
                    reason: format!(
                        "segment base offset {} does not match expected offset {}",
                        segment_file.base_offset.value(),
                        expected_offset.value()
                    ),
                });
            }

            let is_final = Some(index) == final_index;
            let mode = if is_final {
                ScanMode::RECOVER_FINAL
            } else {
                ScanMode::STRICT
            };
            let scan = scan_segment(
                &segment_file.path,
                &self.topic,
                self.partition,
                expected_offset,
                mode,
            )?;

            if scan.size_bytes == 0 && !is_final {
                return Err(StorageError::CorruptSegment {
                    path: segment_file.path,
                    reason: "empty non-final segment".to_owned(),
                });
            }

            self.segments.push(SegmentMetadata {
                base_offset: segment_file.base_offset,
                next_offset: scan.next_offset,
                path: segment_file.path,
                size_bytes: scan.size_bytes,
            });
            expected_offset = scan.next_offset;
        }

        self.next_offset = expected_offset;
        Ok(())
    }

    fn roll_if_needed(&mut self, frame_size: u64) -> StorageResult<()> {
        let Some(active) = self.segments.last() else {
            return Ok(());
        };

        let would_exceed = active
            .size_bytes
            .checked_add(frame_size)
            .is_none_or(|size| size > self.config.max_segment_bytes);
        if active.size_bytes > 0 && would_exceed {
            self.create_active_segment()?;
        }

        Ok(())
    }

    fn ensure_active_segment(&mut self) -> StorageResult<usize> {
        if self.segments.is_empty() {
            self.create_active_segment()?;
        }

        Ok(self.segments.len() - 1)
    }

    fn create_active_segment(&mut self) -> StorageResult<()> {
        let base_offset = self.next_offset;
        let path = segment_path(&self.partition_dir, base_offset);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        self.segments.push(SegmentMetadata {
            base_offset,
            next_offset: base_offset,
            path,
            size_bytes: 0,
        });
        Ok(())
    }
}

#[derive(Debug)]
struct SegmentFile {
    base_offset: Offset,
    path: PathBuf,
}

fn validate_config(config: &LogConfig) -> StorageResult<()> {
    if config.max_segment_bytes == 0 {
        return Err(StorageError::InvalidConfig {
            reason: "max_segment_bytes must be greater than zero".to_owned(),
        });
    }

    Ok(())
}

fn partition_dir(root_dir: &Path, topic: &TopicName, partition: PartitionId) -> PathBuf {
    root_dir
        .join("topics")
        .join(topic.as_str())
        .join("partitions")
        .join(partition.value().to_string())
}

fn segment_path(partition_dir: &Path, base_offset: Offset) -> PathBuf {
    partition_dir.join(format!(
        "{:0SEGMENT_FILE_NAME_WIDTH$}.{SEGMENT_FILE_EXTENSION}",
        base_offset.value()
    ))
}

fn discover_segment_files(partition_dir: &Path) -> StorageResult<Vec<SegmentFile>> {
    let mut segment_files = Vec::new();

    for entry in fs::read_dir(partition_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some(SEGMENT_FILE_EXTENSION)
        {
            continue;
        }

        let base_offset = parse_segment_base_offset(&path)?;
        segment_files.push(SegmentFile { base_offset, path });
    }

    segment_files.sort_by_key(|segment| segment.base_offset);
    Ok(segment_files)
}

fn parse_segment_base_offset(path: &Path) -> StorageResult<Offset> {
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| StorageError::InvalidFormat {
            path: path.to_path_buf(),
            reason: "segment file name is not valid UTF-8".to_owned(),
        })?;

    if stem.len() != SEGMENT_FILE_NAME_WIDTH || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StorageError::InvalidFormat {
            path: path.to_path_buf(),
            reason: "segment file name must be a 20-digit base offset".to_owned(),
        });
    }

    let value = stem
        .parse::<u64>()
        .map_err(|error| StorageError::InvalidFormat {
            path: path.to_path_buf(),
            reason: format!("segment base offset is invalid: {error}"),
        })?;
    Ok(Offset::new(value))
}

fn encode_frame(payload: &[u8], path: &Path) -> StorageResult<Vec<u8>> {
    let record_length = u32::try_from(payload.len()).map_err(|_| StorageError::InvalidFormat {
        path: path.to_path_buf(),
        reason: "record payload exceeds u32 frame length".to_owned(),
    })?;
    let checksum = crc32fast::hash(payload);

    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES as usize + payload.len());
    frame.extend_from_slice(&record_length.to_le_bytes());
    frame.extend_from_slice(&checksum.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn append_frame_with_rollback(path: &Path, frame: &[u8], rollback_len: u64) -> StorageResult<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if let Err(error) = file.write_all(frame).and_then(|()| file.flush()) {
        if let Err(rollback_error) = file.set_len(rollback_len) {
            return Err(StorageError::CorruptSegment {
                path: path.to_path_buf(),
                reason: format!(
                    "append failed ({error}); rollback to byte {rollback_len} failed: {rollback_error}"
                ),
            });
        }
        return Err(StorageError::Io(error));
    }

    Ok(())
}

fn scan_segment(
    path: &Path,
    topic: &TopicName,
    partition: PartitionId,
    start_offset: Offset,
    mode: ScanMode,
) -> StorageResult<SegmentScan> {
    let file_len = fs::metadata(path)?.len();
    let mut file = OpenOptions::new()
        .read(true)
        .write(mode.repair_final_trailing_record)
        .open(path)?;
    let mut position = 0;
    let mut expected_offset = start_offset;
    let mut records = Vec::new();

    while position < file_len {
        let frame_start = position;
        let remaining = file_len - frame_start;
        if remaining < 4 {
            return repair_or_reject_trailing(
                &file,
                path,
                frame_start,
                expected_offset,
                records,
                mode,
                "truncated record length",
            );
        }

        let mut record_length_bytes = [0_u8; 4];
        file.read_exact(&mut record_length_bytes)?;
        let record_length = u64::from(u32::from_le_bytes(record_length_bytes));
        let frame_size = FRAME_HEADER_BYTES
            .checked_add(record_length)
            .ok_or_else(|| StorageError::InvalidFormat {
                path: path.to_path_buf(),
                reason: "frame size overflow".to_owned(),
            })?;

        if remaining < frame_size {
            return repair_or_reject_trailing(
                &file,
                path,
                frame_start,
                expected_offset,
                records,
                mode,
                "truncated record payload",
            );
        }

        let mut checksum_bytes = [0_u8; 4];
        file.read_exact(&mut checksum_bytes)?;
        let expected_checksum = u32::from_le_bytes(checksum_bytes);

        let payload_len =
            usize::try_from(record_length).map_err(|_| StorageError::InvalidFormat {
                path: path.to_path_buf(),
                reason: "record length does not fit in usize".to_owned(),
            })?;
        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        position = frame_start + frame_size;

        let actual_checksum = crc32fast::hash(&payload);
        if expected_checksum != actual_checksum {
            if mode.repair_final_trailing_record && position == file_len {
                return repair_or_reject_trailing(
                    &file,
                    path,
                    frame_start,
                    expected_offset,
                    records,
                    mode,
                    "checksum mismatch in trailing record",
                );
            }

            return Err(StorageError::ChecksumMismatch {
                path: path.to_path_buf(),
                position: frame_start,
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        let persisted = match serde_json::from_slice::<PersistedRecord>(&payload) {
            Ok(record) => record,
            Err(error) if mode.repair_final_trailing_record && position == file_len => {
                return repair_or_reject_trailing(
                    &file,
                    path,
                    frame_start,
                    expected_offset,
                    records,
                    mode,
                    &format!("invalid trailing JSON record: {error}"),
                );
            }
            Err(error) => return Err(StorageError::Serde(error)),
        };

        let record =
            match validate_persisted_record(path, topic, partition, expected_offset, persisted) {
                Ok(record) => record,
                Err(error) if mode.repair_final_trailing_record && position == file_len => {
                    return repair_or_reject_trailing(
                        &file,
                        path,
                        frame_start,
                        expected_offset,
                        records,
                        mode,
                        &format!("invalid trailing record metadata: {error}"),
                    );
                }
                Err(error) => return Err(error),
            };
        expected_offset = increment_offset(expected_offset, path)?;
        records.push(record);
    }

    Ok(SegmentScan {
        records,
        next_offset: expected_offset,
        size_bytes: file_len,
    })
}

fn repair_or_reject_trailing(
    file: &File,
    path: &Path,
    frame_start: u64,
    expected_offset: Offset,
    records: Vec<StoredMessageRecord>,
    mode: ScanMode,
    reason: &str,
) -> StorageResult<SegmentScan> {
    if mode.repair_final_trailing_record {
        file.set_len(frame_start)?;
        return Ok(SegmentScan {
            records,
            next_offset: expected_offset,
            size_bytes: frame_start,
        });
    }

    Err(StorageError::CorruptSegment {
        path: path.to_path_buf(),
        reason: reason.to_owned(),
    })
}

fn validate_persisted_record(
    path: &Path,
    topic: &TopicName,
    partition: PartitionId,
    expected_offset: Offset,
    record: PersistedRecord,
) -> StorageResult<StoredMessageRecord> {
    if record.format_version != FORMAT_VERSION {
        return Err(StorageError::InvalidFormat {
            path: path.to_path_buf(),
            reason: format!(
                "unsupported record format version {}",
                record.format_version
            ),
        });
    }

    if record.topic.as_str() != topic.as_str() {
        return Err(StorageError::CorruptSegment {
            path: path.to_path_buf(),
            reason: format!(
                "record topic {} does not match expected topic {}",
                record.topic, topic
            ),
        });
    }

    if record.partition != partition {
        return Err(StorageError::CorruptSegment {
            path: path.to_path_buf(),
            reason: format!(
                "record partition {} does not match expected partition {}",
                record.partition.value(),
                partition.value()
            ),
        });
    }

    if record.offset != expected_offset {
        return Err(StorageError::CorruptSegment {
            path: path.to_path_buf(),
            reason: format!(
                "record offset {} does not match expected offset {}",
                record.offset.value(),
                expected_offset.value()
            ),
        });
    }

    Ok(StoredMessageRecord {
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        envelope: record.envelope,
    })
}

fn increment_offset(offset: Offset, path: &Path) -> StorageResult<Offset> {
    let next = offset
        .value()
        .checked_add(1)
        .ok_or_else(|| StorageError::InvalidFormat {
            path: path.to_path_buf(),
            reason: "offset overflow".to_owned(),
        })?;
    Ok(Offset::new(next))
}

/// Returns this crate's package name.
#[must_use]
pub fn crate_name() -> &'static str {
    "msg-storage"
}

#[cfg(test)]
mod tests {
    use super::crate_name;

    #[test]
    fn reports_crate_name() {
        assert_eq!(crate_name(), "msg-storage");
    }
}
