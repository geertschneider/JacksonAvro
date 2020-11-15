package be.vdab.vdp.util

import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import groovy.transform.CompileStatic
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.*
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer

import java.nio.ByteBuffer

class  JacksonAvroSerDe<T> implements Serde<T> {
    private final Serde<T> inner;

    JacksonAvroSerDe(Class<T> type){
        AvroMapper avroMapper=new AvroMapper()
        avroMapper.registerModule(new JavaTimeModule())
        inner  = Serdes.serdeFrom(new JacksonAvroSerializer<T>(avroMapper),new JacksonAvroDeSerializer<T>(type,avroMapper))
    }
    JacksonAvroSerDe(Class<T> type,AvroMapper mapper){
        inner  = Serdes.serdeFrom(new JacksonAvroSerializer<T>(mapper),new JacksonAvroDeSerializer<T>(type,mapper))
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
       serializer().configure(new KafkaAvroSerializerConfig(serdeConfig));
       deserializer().configure(new KafkaAvroDeserializerConfig(serdeConfig) );
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

}

@CompileStatic
class  JacksonAvroSerializer<T> extends AbstractKafkaAvroSerializer implements Serializer<T> {
    AvroMapper avroMapper

    JacksonAvroSerializer(AvroMapper mapper){
        this.avroMapper=mapper
    }

    @Override
    protected void configure( KafkaAvroSerializerConfig config) {
        super.configure(config)
    }

    @Override
    byte[] serialize(String topic, T data) {
        //this could be optimized by caching the schema's
        final com.fasterxml.jackson.dataformat.avro.AvroSchema jacksonSchema=avroMapper.schemaFor(data.class)
        final org.apache.avro.Schema avroSchema=jacksonSchema.getAvroSchema()
        avroSchema.type
        final AvroSchema confluentSchema=new AvroSchema(avroSchema)

        byte[] serializedData= avroMapper.writer(jacksonSchema).writeValueAsBytes(data)
println  "****** serialized data : size : ${serializedData.size()} -- content : ${serializedData}"
        serializeImpl(topic,serializedData,confluentSchema)
    }

    @Override
    protected byte[] serializeImpl(String subject, Object object, AvroSchema schema) throws SerializationException {
        if (object == null) {
            return null;
        }
        String restClientErrorMsg = "";
        try {
            int id;
            if (autoRegisterSchema) {
                restClientErrorMsg = "Error registering Avro schema: ";
                id = schemaRegistry.register(subject, schema);
            } else if (useLatestVersion) {
                restClientErrorMsg = "Error retrieving latest version: ";
                schema = (AvroSchema) lookupLatestVersion(subject, schema);
                id = schemaRegistry.getId(subject, schema);
            } else {
                restClientErrorMsg = "Error retrieving Avro schema: ";
                id = schemaRegistry.getId(subject, schema);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            out.write(ByteBuffer.allocate(idSize).putInt(id).array());
            Object value = object instanceof NonRecordContainer
                    ? ((NonRecordContainer) object).getValue()
                    : object;

            out.write((byte[]) value);
            out.flush()
            out.close()

            return out.toByteArray();

        }
         catch (IOException | RuntimeException e) {
            // avro serialization can throw AvroRuntimeException, NullPointerException,
            // ClassCastException, etc
            throw new SerializationException("Error serializing Avro message", e);
        }
        catch (RestClientException e) {
            throw new SerializationException(restClientErrorMsg + schema, e);
        }
    }
}

//
@CompileStatic
class  JacksonAvroDeSerializer<T> extends AbstractKafkaAvroDeserializer implements Deserializer<T> {

    final AvroMapper avroMapper
    Class<T> type
    Integer schemaId

    JacksonAvroDeSerializer(Class<T> type,AvroMapper mapper ){
        this.avroMapper=mapper
        this.type=type
    }

    @Override
    protected void configure(KafkaAvroDeserializerConfig config) {
        super.configure(config)
    }

    AvroSchema schemaFromRegistry() {
        try {
            return (AvroSchema) schemaRegistry.getSchemaById(schemaId);
        } catch (RestClientException | IOException e) {
            throw new SerializationException("Error retrieving Avro schema for id ${schemaId} ", e);
        }
    }


    @Override
    T deserialize(String topic, byte[] data) {

        if (data == null) {
            return null;
        }

        ByteBuffer buffer=getByteBuffer(data)
        this.schemaId = buffer.getInt();
        AvroSchema avroSchema = schemaFromRegistry()

        final DecoderFactory decoderFactory = DecoderFactory.get();

        int length = buffer.limit() - 1 - idSize;
        byte[] bytes = new byte[length];
        ByteBuffer byteBuffer=buffer.get(bytes, 0, length)
        final com.fasterxml.jackson.dataformat.avro.AvroSchema jacksonSchema =new com.fasterxml.jackson.dataformat.avro.AvroSchema(avroSchema.rawSchema())
        return avroMapper.readerFor(type).with(jacksonSchema).readValue(bytes)
    }

}

