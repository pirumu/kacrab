use super::{
    AHashMap, AHashSet, COMPRESSION_RATE_ESTIMATION_FACTOR, Duration, Instant, ProducerError,
    ProducerRecord, RECORD_BATCH_OVERHEAD_BYTES, Result, SharedAccumulator, TopicMetadata,
    estimate_record_batch_bytes, murmur2_java, record,
};

#[derive(Debug, Default)]
pub(crate) struct ProducerPartitionerState {
    pub(crate) next_by_topic: AHashMap<String, i32>,
    pub(crate) sticky_by_topic: AHashMap<String, StickyPartitionState>,
    pub(crate) load_stats_by_topic: AHashMap<String, PartitionLoadStats>,
    pub(crate) broker_drain_stats_by_id: AHashMap<i32, BrokerDrainStats>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StickyPartitionState {
    pub(crate) partition: i32,
    pub(crate) bytes: usize,
    pub(crate) switch_on_next: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TopicPartitionAssignment<'a> {
    pub(crate) topic: &'a str,
    pub(crate) topic_metadata: &'a TopicMetadata,
    pub(crate) ignore_keys: bool,
    pub(crate) adaptive: bool,
    pub(crate) sticky_batch_size: usize,
    pub(crate) compression_ratio: f32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionLoadRefresh<'a> {
    pub(crate) topic: &'a str,
    pub(crate) topic_metadata: &'a TopicMetadata,
    pub(crate) accumulator: &'a SharedAccumulator,
    pub(crate) now: Instant,
    pub(crate) availability_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionLoadStats {
    pub(crate) cumulative_frequency_table: Vec<i32>,
    pub(crate) partition_ids: Vec<i32>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BrokerDrainStats {
    pub(crate) ready_at: Instant,
    pub(crate) drain_at: Instant,
    pub(crate) in_flight: usize,
}

impl ProducerPartitionerState {
    pub(crate) fn next_for_topic(&mut self, topic: &str) -> &mut i32 {
        self.next_by_topic.entry(topic.to_owned()).or_insert(0)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Partition choice needs record data, metadata, and current partitioner policies."
    )]
    pub(crate) fn partition_for_record(
        &mut self,
        metadata: &crate::wire::ClusterMetadata,
        record: &ProducerRecord,
        ignore_keys: bool,
        adaptive: bool,
        sticky_batch_size: usize,
        compression_ratio: f32,
    ) -> Result<i32> {
        if record.has_assigned_partition() {
            return Ok(record.partition);
        }
        if !ignore_keys && record.key.is_some() {
            let topic_metadata = Self::topic_metadata(metadata, &record.topic, record.partition)?;
            return key_partition(&record.topic, record.partition, topic_metadata, record);
        }
        let topic_metadata = Self::topic_metadata(metadata, &record.topic, record.partition)?;
        self.sticky_partition(
            &record.topic,
            topic_metadata,
            record,
            adaptive,
            sticky_batch_size,
            compression_ratio,
        )
    }

    #[cfg(test)]
    #[expect(
        clippy::too_many_arguments,
        reason = "Test-only entrypoint pins ratio-aware sticky sizing without constructing a \
                  dispatcher."
    )]
    pub(crate) fn partition_for_record_with_compression_ratio(
        &mut self,
        metadata: &crate::wire::ClusterMetadata,
        record: &ProducerRecord,
        ignore_keys: bool,
        adaptive: bool,
        sticky_batch_size: usize,
        compression_ratio: f32,
    ) -> Result<i32> {
        self.partition_for_record(
            metadata,
            record,
            ignore_keys,
            adaptive,
            sticky_batch_size,
            compression_ratio,
        )
    }

    pub(crate) fn try_assign_cached_sticky_partition(
        &mut self,
        record: &mut ProducerRecord,
        sticky_batch_size: usize,
        compression_ratio: f32,
    ) -> bool {
        if record.has_assigned_partition() {
            return true;
        }
        let Some(sticky) = self.sticky_by_topic.get_mut(record.topic.as_ref()) else {
            return false;
        };
        if sticky.switch_on_next {
            return false;
        }
        let record_bytes = estimate_sticky_record_bytes(record, compression_ratio);
        let next_bytes = sticky.bytes.saturating_add(record_bytes);
        if next_bytes >= sticky_batch_size.max(1).saturating_mul(2) {
            return false;
        }
        record.partition = sticky.partition;
        sticky.bytes = next_bytes;
        true
    }

    pub(crate) fn assign_topic_partitions(
        &mut self,
        assignment: TopicPartitionAssignment<'_>,
        records: &mut [ProducerRecord],
    ) -> Result<()> {
        let TopicPartitionAssignment {
            topic,
            topic_metadata,
            ignore_keys,
            adaptive,
            sticky_batch_size,
            compression_ratio,
        } = assignment;
        ensure_partitions(topic, record::UNASSIGNED_PARTITION, topic_metadata)?;
        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            },
        };
        let mut sticky_used = false;

        for record in records {
            if record.has_assigned_partition() || record.topic.as_ref() != topic {
                continue;
            }
            if !ignore_keys && record.key.is_some() {
                record.partition = key_partition(topic, record.partition, topic_metadata, record)?;
                continue;
            }

            if sticky.switch_on_next {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
            sticky_used = true;
            record.partition = sticky.partition;
            sticky.bytes = sticky
                .bytes
                .saturating_add(estimate_sticky_record_bytes(record, compression_ratio));
            if sticky.bytes >= sticky_batch_size.max(1).saturating_mul(2) {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
        }

        if sticky_used {
            let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn assign_sticky_topic_partitions(
        &mut self,
        assignment: TopicPartitionAssignment<'_>,
        records: &mut [ProducerRecord],
    ) -> Result<()> {
        let TopicPartitionAssignment {
            topic,
            topic_metadata,
            adaptive,
            sticky_batch_size,
            compression_ratio,
            ..
        } = assignment;
        ensure_partitions(topic, record::UNASSIGNED_PARTITION, topic_metadata)?;
        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            },
        };

        for record in records {
            if sticky.switch_on_next {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
            record.partition = sticky.partition;
            sticky.bytes = sticky
                .bytes
                .saturating_add(estimate_sticky_record_bytes(record, compression_ratio));
            if sticky.bytes >= sticky_batch_size.max(1).saturating_mul(2) {
                sticky = StickyPartitionState {
                    partition: self.next_partition(topic, topic_metadata, adaptive)?,
                    bytes: RECORD_BATCH_OVERHEAD_BYTES,
                    switch_on_next: false,
                };
            }
        }

        let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Sticky partitioning combines metadata, record size, and active policy knobs."
    )]
    pub(crate) fn sticky_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        record: &ProducerRecord,
        adaptive: bool,
        sticky_batch_size: usize,
        compression_ratio: f32,
    ) -> Result<i32> {
        ensure_partitions(topic, record.partition, topic_metadata)?;

        let existing_sticky = self.valid_sticky(topic, topic_metadata);
        let mut sticky = match existing_sticky {
            Some(sticky) => sticky,
            None => StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            },
        };

        if sticky.switch_on_next {
            sticky = StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            };
        }
        let partition = sticky.partition;
        sticky.bytes = sticky
            .bytes
            .saturating_add(estimate_sticky_record_bytes(record, compression_ratio));
        if sticky.bytes >= sticky_batch_size.max(1).saturating_mul(2) {
            sticky = StickyPartitionState {
                partition: self.next_partition(topic, topic_metadata, adaptive)?,
                bytes: RECORD_BATCH_OVERHEAD_BYTES,
                switch_on_next: false,
            };
        }
        let _previous = self.sticky_by_topic.insert(topic.to_owned(), sticky);
        Ok(partition)
    }

    pub(crate) fn mark_sticky_batch_ready(&mut self, topic: &str, sticky_batch_size: usize) {
        let Some(sticky) = self.sticky_by_topic.get_mut(topic) else {
            return;
        };
        if sticky.bytes >= sticky_batch_size.max(1) {
            sticky.switch_on_next = true;
        }
    }

    pub(crate) fn next_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        adaptive: bool,
    ) -> Result<i32> {
        ensure_partitions(topic, record::UNASSIGNED_PARTITION, topic_metadata)?;
        if adaptive && let Some(partition) = self.next_adaptive_partition(topic, topic_metadata) {
            return Ok(partition);
        }
        let random = self.next_partition_counter(topic);
        uniform_partition_for_random(topic, topic_metadata, random)
    }

    pub(crate) fn next_adaptive_partition(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
    ) -> Option<i32> {
        let random = self.next_partition_counter(topic);
        self.adaptive_partition_for_random(topic, topic_metadata, random)
    }

    pub(crate) fn adaptive_partition_for_random(
        &self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        random: usize,
    ) -> Option<i32> {
        let range_end = self
            .load_stats_by_topic
            .get(topic)?
            .cumulative_frequency_table
            .last()
            .copied()?;
        let range_end = usize::try_from(range_end).ok()?.max(1);
        let weighted = i32::try_from(random.checked_rem(range_end).unwrap_or(0)).ok()?;
        let stats = self.load_stats_by_topic.get(topic)?;
        let partition = stats
            .cumulative_frequency_table
            .iter()
            .position(|limit| weighted < *limit)
            .and_then(|index| stats.partition_ids.get(index).copied())?;
        topic_metadata
            .partitions
            .iter()
            .any(|metadata| metadata.partition_index == partition)
            .then_some(partition)
    }

    pub(crate) fn update_partition_load_stats(
        &mut self,
        topic: &str,
        queue_sizes: &[i32],
        partition_ids: &[i32],
        length: usize,
    ) {
        let Some(stats) = build_partition_load_stats(queue_sizes, partition_ids, length) else {
            let _removed = self.load_stats_by_topic.remove(topic);
            return;
        };
        let _previous = self.load_stats_by_topic.insert(topic.to_owned(), stats);
    }

    #[cfg(test)]
    pub(crate) fn update_partition_load_stats_from_accumulator(
        &mut self,
        topic: &str,
        topic_metadata: &TopicMetadata,
        accumulator: &SharedAccumulator,
    ) {
        self.update_partition_load_stats_from_accumulator_at(PartitionLoadRefresh {
            topic,
            topic_metadata,
            accumulator,
            now: Instant::now(),
            availability_timeout: Duration::ZERO,
        });
    }

    pub(crate) fn update_partition_load_stats_from_accumulator_at(
        &mut self,
        refresh: PartitionLoadRefresh<'_>,
    ) {
        let unavailable_brokers = self.unavailable_topic_leaders(
            refresh.topic_metadata,
            refresh.now,
            refresh.availability_timeout,
        );
        let Some(load) = refresh
            .accumulator
            .partition_queue_load_with_availability(refresh.topic_metadata, |partition| {
                !unavailable_brokers.contains(&partition.leader_id)
            })
        else {
            let _removed = self.load_stats_by_topic.remove(refresh.topic);
            return;
        };
        self.update_partition_load_stats(
            refresh.topic,
            &load.queue_sizes,
            &load.partition_ids,
            load.length,
        );
    }

    #[cfg(test)]
    pub(crate) fn update_broker_drain_stats(
        &mut self,
        broker_id: i32,
        now: Instant,
        can_drain: bool,
    ) {
        self.update_broker_latency_stats(broker_id, now, can_drain);
    }

    pub(crate) fn update_broker_latency_stats(
        &mut self,
        broker_id: i32,
        now: Instant,
        can_drain: bool,
    ) {
        let stats = self
            .broker_drain_stats_by_id
            .entry(broker_id)
            .or_insert(BrokerDrainStats {
                ready_at: now,
                drain_at: now,
                in_flight: 0,
            });
        if can_drain {
            stats.drain_at = now;
        }
        stats.ready_at = now;
    }

    pub(crate) fn record_broker_drain_started(&mut self, broker_id: i32, now: Instant) {
        self.update_broker_latency_stats(broker_id, now, true);
        let stats = self
            .broker_drain_stats_by_id
            .entry(broker_id)
            .or_insert(BrokerDrainStats {
                ready_at: now,
                drain_at: now,
                in_flight: 0,
            });
        stats.in_flight = stats.in_flight.saturating_add(1);
    }

    pub(crate) fn record_broker_drain_finished(&mut self, broker_id: i32, now: Instant) {
        let stats = self
            .broker_drain_stats_by_id
            .entry(broker_id)
            .or_insert(BrokerDrainStats {
                ready_at: now,
                drain_at: now,
                in_flight: 0,
            });
        stats.in_flight = stats.in_flight.saturating_sub(1);
        if stats.in_flight == 0 {
            stats.drain_at = now;
            stats.ready_at = now;
        }
    }

    pub(crate) fn unavailable_topic_leaders(
        &mut self,
        topic_metadata: &TopicMetadata,
        now: Instant,
        availability_timeout: Duration,
    ) -> AHashSet<i32> {
        if availability_timeout.is_zero() {
            return AHashSet::new();
        }
        let mut unavailable = AHashSet::new();
        for partition in &topic_metadata.partitions {
            if partition.leader_id < 0 {
                continue;
            }
            if let Some(stats) = self.broker_drain_stats_by_id.get_mut(&partition.leader_id) {
                if stats.in_flight > 0 {
                    stats.ready_at = now;
                }
                let waiting = stats
                    .ready_at
                    .checked_duration_since(stats.drain_at)
                    .unwrap_or(Duration::ZERO);
                if waiting > availability_timeout {
                    let _inserted = unavailable.insert(partition.leader_id);
                }
            }
        }
        unavailable
    }

    pub(crate) fn topic_metadata<'a>(
        metadata: &'a crate::wire::ClusterMetadata,
        topic: &str,
        partition: i32,
    ) -> Result<&'a TopicMetadata> {
        metadata
            .topic(topic)
            .ok_or_else(|| ProducerError::UnknownTopic(topic.to_owned()))
            .and_then(|topic_metadata| {
                ensure_partitions(topic, partition, topic_metadata)?;
                Ok(topic_metadata)
            })
    }

    pub(crate) fn valid_sticky(
        &self,
        topic: &str,
        topic_metadata: &TopicMetadata,
    ) -> Option<StickyPartitionState> {
        self.sticky_by_topic.get(topic).copied().filter(|sticky| {
            topic_metadata
                .partitions
                .iter()
                .any(|partition| partition.partition_index == sticky.partition)
        })
    }

    pub(crate) fn next_partition_counter(&mut self, topic: &str) -> usize {
        let next_round_robin = self.next_for_topic(topic);
        let next = usize::try_from(*next_round_robin).unwrap_or(0);
        *next_round_robin = next_round_robin
            .checked_add(1)
            .filter(|value| *value >= 0)
            .unwrap_or(0);
        next
    }
}

fn ensure_partitions(topic: &str, partition: i32, topic_metadata: &TopicMetadata) -> Result<()> {
    if topic_metadata.partitions.is_empty() {
        return Err(ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition,
        });
    }
    Ok(())
}

fn key_partition(
    topic: &str,
    partition: i32,
    topic_metadata: &TopicMetadata,
    record: &ProducerRecord,
) -> Result<i32> {
    ensure_partitions(topic, partition, topic_metadata)?;
    let Some(key) = record.key.as_ref() else {
        return Err(ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition,
        });
    };
    let partition_count = topic_metadata.partitions.len();
    let hash = usize::try_from(murmur2_java(key.as_ref()) & 0x7fff_ffff).unwrap_or(0);
    let offset = hash.checked_rem(partition_count).unwrap_or(0);
    topic_metadata
        .partitions
        .get(offset)
        .map(|partition_metadata| partition_metadata.partition_index)
        .ok_or_else(|| ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition,
        })
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Kafka compression ratio estimates are f32 and only influence sticky byte budgets."
)]
pub(crate) fn estimate_sticky_record_bytes(
    record: &ProducerRecord,
    compression_ratio: f32,
) -> usize {
    let ratio = if compression_ratio.is_finite() && compression_ratio > 0.0 {
        compression_ratio
    } else {
        1.0
    };
    ((estimate_record_batch_bytes(record) as f32) * ratio * COMPRESSION_RATE_ESTIMATION_FACTOR)
        .ceil() as usize
}

pub(crate) fn uniform_partition_for_random(
    topic: &str,
    topic_metadata: &TopicMetadata,
    random: usize,
) -> Result<i32> {
    ensure_partitions(topic, record::UNASSIGNED_PARTITION, topic_metadata)?;
    let available_count = topic_metadata
        .partitions
        .iter()
        .filter(|partition| partition.leader_id >= 0)
        .count();
    let partition_count = if available_count > 0 {
        available_count
    } else {
        topic_metadata.partitions.len()
    };
    let offset = random.checked_rem(partition_count).unwrap_or(0);
    let selected = if available_count > 0 {
        topic_metadata
            .partitions
            .iter()
            .filter(|partition| partition.leader_id >= 0)
            .nth(offset)
    } else {
        topic_metadata.partitions.get(offset)
    };
    selected
        .map(|partition| partition.partition_index)
        .ok_or_else(|| ProducerError::UnknownPartition {
            topic: topic.to_owned(),
            partition: record::UNASSIGNED_PARTITION,
        })
}

pub(crate) fn build_partition_load_stats(
    queue_sizes: &[i32],
    partition_ids: &[i32],
    length: usize,
) -> Option<PartitionLoadStats> {
    if queue_sizes.len() != partition_ids.len() || length == 0 || length > queue_sizes.len() {
        return None;
    }
    if queue_sizes.len() < 2 {
        return None;
    }
    let logical_sizes = queue_sizes.get(..length)?;
    let logical_partitions = partition_ids.get(..length)?;
    let first = *logical_sizes.first()?;
    let mut max_size = first;
    let mut all_equal = true;
    for size in logical_sizes.iter().copied().skip(1) {
        if size != first {
            all_equal = false;
        }
        if size > max_size {
            max_size = size;
        }
    }
    if all_equal && length == queue_sizes.len() {
        return None;
    }
    let max_size_plus_one = max_size.checked_add(1)?;
    let mut cumulative_frequency_table = Vec::with_capacity(length);
    let mut running = 0i32;
    for size in logical_sizes.iter().copied() {
        let frequency = max_size_plus_one.checked_sub(size)?;
        running = running.checked_add(frequency)?;
        cumulative_frequency_table.push(running);
    }
    if running <= 0 {
        return None;
    }
    Some(PartitionLoadStats {
        cumulative_frequency_table,
        partition_ids: logical_partitions.to_vec(),
    })
}
