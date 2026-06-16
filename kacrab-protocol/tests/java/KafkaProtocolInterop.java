import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKeyCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKeyCollection;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;

public final class KafkaProtocolInterop {
    private static final short API_VERSIONS_REQUEST_VERSION = 3;
    private static final short API_VERSIONS_RESPONSE_VERSION = 4;
    private static final short METADATA_REQUEST_VERSION = 12;

    private KafkaProtocolInterop() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("missing command");
        }
        switch (args[0]) {
            case "roundtrip-hex":
                requireArgs(args, 4);
                System.out.println(roundtripHex(args[1], Short.parseShort(args[2]), args[3]));
                return;
            case "encode-default":
                requireArgs(args, 3);
                System.out.println(encodeDefault(args[1], Short.parseShort(args[2])));
                return;
            case "encode-api-versions-request-v3":
                System.out.println(encode(apiVersionsRequestFixture(), API_VERSIONS_REQUEST_VERSION));
                return;
            case "decode-api-versions-request-v3":
                requireArgs(args, 2);
                assertApiVersionsRequest(decodeApiVersionsRequest(args[1], API_VERSIONS_REQUEST_VERSION));
                System.out.println("ok");
                return;
            case "encode-api-versions-response-v4":
                System.out.println(encode(apiVersionsResponseFixture(), API_VERSIONS_RESPONSE_VERSION));
                return;
            case "decode-api-versions-response-v4":
                requireArgs(args, 2);
                assertApiVersionsResponse(decodeApiVersionsResponse(args[1], API_VERSIONS_RESPONSE_VERSION));
                System.out.println("ok");
                return;
            case "encode-metadata-request-v12":
                System.out.println(encode(metadataRequestFixture(), METADATA_REQUEST_VERSION));
                return;
            case "decode-metadata-request-v12":
                requireArgs(args, 2);
                assertMetadataRequest(decodeMetadataRequest(args[1], METADATA_REQUEST_VERSION));
                System.out.println("ok");
                return;
            default:
                throw new IllegalArgumentException("unknown command: " + args[0]);
        }
    }

    private static String roundtripHex(String className, short version, String hex) throws Exception {
        return encode(decodeMessage(className, version, hex), version);
    }

    private static String encodeDefault(String className, short version) throws Exception {
        Message message = (Message) Class.forName(className).getConstructor().newInstance();
        return encode(message, version);
    }

    private static Message decodeMessage(String className, short version, String hex) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(decodeHex(hex));
        Constructor<?> constructor = Class
            .forName(className)
            .getConstructor(org.apache.kafka.common.protocol.Readable.class, short.class);
        Message message = (Message) constructor.newInstance(new ByteBufferAccessor(buffer), version);
        assertConsumed(buffer);
        return message;
    }

    private static void requireArgs(String[] args, int expected) {
        if (args.length != expected) {
            throw new IllegalArgumentException("expected " + expected + " args, got " + args.length);
        }
    }

    private static ApiVersionsRequestData apiVersionsRequestFixture() {
        ApiVersionsRequestData data = new ApiVersionsRequestData()
            .setClientSoftwareName("kacrab")
            .setClientSoftwareVersion("0.0.1");
        data.unknownTaggedFields().add(new RawTaggedField(9, bytes("client-tag")));
        return data;
    }

    private static ApiVersionsResponseData apiVersionsResponseFixture() {
        ApiVersion apiVersions = new ApiVersion()
            .setApiKey((short) 18)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 4);
        apiVersions.unknownTaggedFields().add(new RawTaggedField(1, bytes("api-tag")));

        ApiVersion metadata = new ApiVersion()
            .setApiKey((short) 3)
            .setMinVersion((short) 0)
            .setMaxVersion((short) 13);

        SupportedFeatureKey supportedFeature = new SupportedFeatureKey()
            .setName("metadata.version")
            .setMinVersion((short) 1)
            .setMaxVersion((short) 23);
        supportedFeature.unknownTaggedFields().add(new RawTaggedField(2, bytes("supported-tag")));

        FinalizedFeatureKey finalizedFeature = new FinalizedFeatureKey()
            .setName("metadata.version")
            .setMaxVersionLevel((short) 23)
            .setMinVersionLevel((short) 1);
        finalizedFeature.unknownTaggedFields().add(new RawTaggedField(3, bytes("finalized-tag")));

        ApiVersionsResponseData data = new ApiVersionsResponseData()
            .setErrorCode((short) 0)
            .setApiKeys(new ApiVersionCollection(Arrays.asList(apiVersions, metadata).iterator()))
            .setThrottleTimeMs(12)
            .setSupportedFeatures(new SupportedFeatureKeyCollection(Arrays.asList(supportedFeature).iterator()))
            .setFinalizedFeaturesEpoch(42L)
            .setFinalizedFeatures(new FinalizedFeatureKeyCollection(Arrays.asList(finalizedFeature).iterator()))
            .setZkMigrationReady(true);
        data.unknownTaggedFields().add(new RawTaggedField(9, bytes("response-tag")));
        return data;
    }

    private static MetadataRequestData metadataRequestFixture() {
        List<MetadataRequestTopic> topics = new ArrayList<>();
        MetadataRequestTopic topicA = new MetadataRequestTopic()
            .setTopicId(new Uuid(0x0102030405060708L, 0x1112131415161718L))
            .setName("topic-a");
        topicA.unknownTaggedFields().add(new RawTaggedField(2, bytes("topic-tag")));
        topics.add(topicA);
        topics.add(new MetadataRequestTopic()
            .setTopicId(new Uuid(0x2122232425262728L, 0x3132333435363738L))
            .setName(null));

        MetadataRequestData data = new MetadataRequestData()
            .setTopics(topics)
            .setAllowAutoTopicCreation(true)
            .setIncludeClusterAuthorizedOperations(false)
            .setIncludeTopicAuthorizedOperations(true);
        data.unknownTaggedFields().add(new RawTaggedField(4, bytes("metadata-tag")));
        return data;
    }

    private static String encode(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        message.write(new ByteBufferAccessor(buffer), cache, version);
        if (buffer.position() != size) {
            throw new AssertionError("encoded size mismatch: expected " + size + ", wrote " + buffer.position());
        }
        return hex(Arrays.copyOf(buffer.array(), buffer.position()));
    }

    private static ApiVersionsRequestData decodeApiVersionsRequest(String hex, short version) {
        ByteBuffer buffer = ByteBuffer.wrap(decodeHex(hex));
        ApiVersionsRequestData data = new ApiVersionsRequestData(new ByteBufferAccessor(buffer), version);
        assertConsumed(buffer);
        return data;
    }

    private static ApiVersionsResponseData decodeApiVersionsResponse(String hex, short version) {
        ByteBuffer buffer = ByteBuffer.wrap(decodeHex(hex));
        ApiVersionsResponseData data = new ApiVersionsResponseData(new ByteBufferAccessor(buffer), version);
        assertConsumed(buffer);
        return data;
    }

    private static MetadataRequestData decodeMetadataRequest(String hex, short version) {
        ByteBuffer buffer = ByteBuffer.wrap(decodeHex(hex));
        MetadataRequestData data = new MetadataRequestData(new ByteBufferAccessor(buffer), version);
        assertConsumed(buffer);
        return data;
    }

    private static void assertConsumed(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            throw new AssertionError("decoder left " + buffer.remaining() + " byte(s)");
        }
    }

    private static void assertApiVersionsRequest(ApiVersionsRequestData data) {
        assertEquals("clientSoftwareName", "kacrab", data.clientSoftwareName());
        assertEquals("clientSoftwareVersion", "0.0.1", data.clientSoftwareVersion());
        assertTaggedField("apiVersions unknown tag", data.unknownTaggedFields(), 9, bytes("client-tag"));
    }

    private static void assertApiVersionsResponse(ApiVersionsResponseData data) {
        assertEquals("errorCode", (short) 0, data.errorCode());
        assertEquals("throttleTimeMs", 12, data.throttleTimeMs());
        assertEquals("apiKeys size", 2, data.apiKeys().size());
        ApiVersion apiVersions = data.apiKeys().find((short) 18);
        if (apiVersions == null) {
            throw new AssertionError("missing ApiVersions api key entry");
        }
        assertEquals("ApiVersions minVersion", (short) 0, apiVersions.minVersion());
        assertEquals("ApiVersions maxVersion", (short) 4, apiVersions.maxVersion());
        assertTaggedField("ApiVersions api key unknown tag", apiVersions.unknownTaggedFields(), 1, bytes("api-tag"));

        ApiVersion metadata = data.apiKeys().find((short) 3);
        if (metadata == null) {
            throw new AssertionError("missing Metadata api key entry");
        }
        assertEquals("Metadata maxVersion", (short) 13, metadata.maxVersion());

        assertEquals("supportedFeatures size", 1, data.supportedFeatures().size());
        SupportedFeatureKey supported = data.supportedFeatures().find("metadata.version");
        if (supported == null) {
            throw new AssertionError("missing supported feature");
        }
        assertEquals("supported minVersion", (short) 1, supported.minVersion());
        assertEquals("supported maxVersion", (short) 23, supported.maxVersion());
        assertTaggedField("supported feature unknown tag", supported.unknownTaggedFields(), 2, bytes("supported-tag"));

        assertEquals("finalizedFeaturesEpoch", 42L, data.finalizedFeaturesEpoch());
        assertEquals("finalizedFeatures size", 1, data.finalizedFeatures().size());
        FinalizedFeatureKey finalized = data.finalizedFeatures().find("metadata.version");
        if (finalized == null) {
            throw new AssertionError("missing finalized feature");
        }
        assertEquals("finalized minVersionLevel", (short) 1, finalized.minVersionLevel());
        assertEquals("finalized maxVersionLevel", (short) 23, finalized.maxVersionLevel());
        assertTaggedField(
            "finalized feature unknown tag",
            finalized.unknownTaggedFields(),
            3,
            bytes("finalized-tag")
        );
        assertEquals("zkMigrationReady", true, data.zkMigrationReady());
        assertTaggedField("response unknown tag", data.unknownTaggedFields(), 9, bytes("response-tag"));
    }

    private static void assertMetadataRequest(MetadataRequestData data) {
        if (data.topics().size() != 2) {
            throw new AssertionError("expected 2 metadata topics, got " + data.topics().size());
        }
        MetadataRequestTopic first = data.topics().get(0);
        assertEquals("topicA id", new Uuid(0x0102030405060708L, 0x1112131415161718L), first.topicId());
        assertEquals("topicA name", "topic-a", first.name());
        assertTaggedField("topicA unknown tag", first.unknownTaggedFields(), 2, bytes("topic-tag"));

        MetadataRequestTopic second = data.topics().get(1);
        assertEquals("topicB id", new Uuid(0x2122232425262728L, 0x3132333435363738L), second.topicId());
        assertEquals("topicB name", null, second.name());
        assertEquals("allowAutoTopicCreation", true, data.allowAutoTopicCreation());
        assertEquals(
            "includeClusterAuthorizedOperations",
            false,
            data.includeClusterAuthorizedOperations()
        );
        assertEquals("includeTopicAuthorizedOperations", true, data.includeTopicAuthorizedOperations());
        assertTaggedField("metadata unknown tag", data.unknownTaggedFields(), 4, bytes("metadata-tag"));
    }

    private static void assertTaggedField(String label, List<RawTaggedField> fields, int tag, byte[] data) {
        if (fields.size() != 1) {
            throw new AssertionError(label + ": expected 1 tag, got " + fields.size());
        }
        RawTaggedField field = fields.get(0);
        assertEquals(label + " id", tag, field.tag());
        if (!Arrays.equals(data, field.data())) {
            throw new AssertionError(label + " data mismatch");
        }
    }

    private static void assertEquals(String label, Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError(label + ": expected " + expected + ", got " + actual);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            out.append(String.format("%02x", value & 0xff));
        }
        return out.toString();
    }

    private static byte[] decodeHex(String input) {
        if ((input.length() & 1) != 0) {
            throw new IllegalArgumentException("hex input length must be even");
        }
        byte[] out = new byte[input.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int index = i * 2;
            out[i] = (byte) Integer.parseInt(input.substring(index, index + 2), 16);
        }
        return out;
    }
}
