//! Generated request/response adapter traits for broker sessions.

use bytes::{Bytes, BytesMut};
use kacrab_protocol::generated::{
    AddPartitionsToTxnRequestData, AddPartitionsToTxnResponseData, ApiVersionsRequestData,
    ApiVersionsResponseData, EndTxnRequestData, EndTxnResponseData, FindCoordinatorRequestData,
    FindCoordinatorResponseData, InitProducerIdRequestData, InitProducerIdResponseData,
    MetadataRequestData, MetadataResponseData, ProduceRequestData, ProduceResponseData,
};

use super::error::Result;

/// A generated Kafka request body that can be encoded by the wire client.
pub trait RequestMessage {
    /// Encode this request body for `version`.
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()>;
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
}

impl ResponseMessage for ApiVersionsResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}

impl RequestMessage for MetadataRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }
}

impl ResponseMessage for MetadataResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}

impl RequestMessage for ProduceRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }
}

impl ResponseMessage for ProduceResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}

impl RequestMessage for InitProducerIdRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }
}

impl ResponseMessage for InitProducerIdResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}

impl RequestMessage for FindCoordinatorRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }
}

impl ResponseMessage for FindCoordinatorResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}

impl RequestMessage for AddPartitionsToTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }
}

impl ResponseMessage for AddPartitionsToTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}

impl RequestMessage for EndTxnRequestData {
    fn write_request(&self, buf: &mut BytesMut, version: i16) -> Result<()> {
        self.write(buf, version)?;
        Ok(())
    }
}

impl ResponseMessage for EndTxnResponseData {
    fn read_response(buf: &mut Bytes, version: i16) -> Result<Self> {
        Ok(Self::read(buf, version)?)
    }
}
