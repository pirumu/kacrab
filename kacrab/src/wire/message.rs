//! Generated request/response adapter traits for broker sessions.

use bytes::{Bytes, BytesMut};
use kacrab_protocol::{
    KafkaString, KafkaUuid, Result,
    generated::{
        AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData, AddPartitionsToTxnRequestData,
        AddPartitionsToTxnResponseData, ApiVersionsRequestData, ApiVersionsResponseData,
        EndTxnRequestData, EndTxnResponseData, FindCoordinatorRequestData,
        FindCoordinatorResponseData, GetTelemetrySubscriptionsRequestData,
        GetTelemetrySubscriptionsResponseData, InitProducerIdRequestData,
        InitProducerIdResponseData, MetadataRequestData, MetadataResponseData, ProduceRequestData,
        ProduceResponseData, PushTelemetryRequestData, PushTelemetryResponseData,
        TxnOffsetCommitRequestData, TxnOffsetCommitResponseData,
    },
};

/// A generated Kafka request body that can be encoded by the wire client.
pub trait RequestMessage {
    /// Encode this request body for `version`.
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()>;

    /// Return the exact encoded body length for `version`.
    fn encoded_len(&self, version: i16) -> Result<usize>;
}

/// A generated Kafka response body that can be decoded by the wire client.
pub trait ResponseMessage: Sized {
    /// Decode this response body for `version`.
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self>;
}

impl RequestMessage for ApiVersionsRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for ApiVersionsResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for MetadataRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for MetadataResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for ProduceRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        normalize_produce_request(self, version).write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        normalize_produce_request(self, version).encoded_len(version)
    }
}

impl ResponseMessage for ProduceResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for InitProducerIdRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for InitProducerIdResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for FindCoordinatorRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for FindCoordinatorResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for AddPartitionsToTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for AddPartitionsToTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for AddOffsetsToTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for AddOffsetsToTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for TxnOffsetCommitRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for TxnOffsetCommitResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for EndTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for EndTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for GetTelemetrySubscriptionsRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for GetTelemetrySubscriptionsResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

impl RequestMessage for PushTelemetryRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> Result<usize> {
        self.encoded_len(version)
    }
}

impl ResponseMessage for PushTelemetryResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Self::read(buf, version)
    }
}

fn normalize_produce_request(request: &ProduceRequestData, version: i16) -> ProduceRequestData {
    let mut normalized = request.clone();
    for topic in &mut normalized.topic_data {
        if version >= 13 {
            topic.name = KafkaString::default();
        } else {
            topic.topic_id = KafkaUuid::ZERO;
        }
    }
    normalized
}
