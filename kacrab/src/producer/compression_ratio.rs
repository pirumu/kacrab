//! Compression ratio estimates used to mirror Kafka producer batch splitting behavior.

use ahash::AHashMap;
use kacrab_protocol::compression::Compression;

const INITIAL_COMPRESSION_RATIO: f32 = 1.0;
#[cfg(test)]
const COMPRESSION_RATIO_IMPROVING_STEP: f32 = 0.005;
#[cfg(test)]
const COMPRESSION_RATIO_DETERIORATE_STEP: f32 = 0.05;

#[derive(Debug, Default, Clone)]
pub(crate) struct CompressionRatioEstimator {
    estimates: AHashMap<CompressionRatioKey, f32>,
}

impl CompressionRatioEstimator {
    pub(crate) fn estimation(&self, topic: &str, codec: Compression) -> f32 {
        self.estimates
            .get(&CompressionRatioKey::new(topic, codec))
            .copied()
            .unwrap_or(INITIAL_COMPRESSION_RATIO)
    }

    #[cfg(test)]
    pub(crate) fn update_estimation(
        &mut self,
        topic: &str,
        codec: Compression,
        observed_ratio: f32,
    ) -> f32 {
        let current = self.estimation(topic, codec);
        let next = if observed_ratio > current {
            (current + COMPRESSION_RATIO_DETERIORATE_STEP).max(observed_ratio)
        } else if observed_ratio < current {
            (current - COMPRESSION_RATIO_IMPROVING_STEP).max(observed_ratio)
        } else {
            current
        };
        self.set_estimation(topic, codec, next);
        next
    }

    pub(crate) fn reset_after_split(
        &mut self,
        topic: &str,
        codec: Compression,
        observed_ratio: f32,
    ) {
        self.set_estimation(topic, codec, observed_ratio.max(INITIAL_COMPRESSION_RATIO));
    }

    pub(crate) fn set_estimation(&mut self, topic: &str, codec: Compression, ratio: f32) {
        let _old = self
            .estimates
            .insert(CompressionRatioKey::new(topic, codec), ratio);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompressionRatioKey {
    topic: String,
    codec: i16,
}

impl CompressionRatioKey {
    fn new(topic: &str, codec: Compression) -> Self {
        Self {
            topic: topic.to_owned(),
            codec: codec as i16,
        }
    }
}
