import be.vdab.vdp.util.JacksonAvroDeSerializer
import be.vdab.vdp.util.JacksonAvroSerDe
import be.vdab.vdp.util.JacksonAvroSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import groovy.transform.CompileStatic
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import scala.Int

import java.time.Duration

@CompileStatic
class Application {
    static void main(String[] args){
        runProducer()
        def gotAll= runConsumer()
        println "got all the records : ${gotAll}"
    }


    public static boolean runConsumer(){
        Consumer<String, User> consumer = createConsumer()
Integer amountOfRecords=0

        (1..5).each{

            ConsumerRecords<String,User> consumedRecords= consumer.poll( Duration.ofSeconds(1,0))
            consumedRecords.each {consumerRecord ->
                println consumerRecord.value()
                amountOfRecords++
            }


            if(amountOfRecords==2)
                return true
        }
        return false

    }

    public static void runProducer(){

        Producer<String, User> producer = createProducer()
        (1..2).each({
            User usr = new User(id: 1, naam: 'Geert', vacatures: [new Vacature(id: it)])
            producer.send(new ProducerRecord<String, User>('testtopic', 'A', usr))

        })
        producer.flush()
        producer.close()
    }

@CompileStatic
    public static Producer<String, User> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 'localhost:9092')
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 'http://localhost:8081')
        props.put(ProducerConfig.CLIENT_ID_CONFIG, 'testClient');
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        AvroMapper mapper = new AvroMapper()
        mapper.registerModule(new JavaTimeModule())
        JacksonAvroSerDe<User> jacksonSerDe= new JacksonAvroSerDe<User>(User.class, mapper)

        HashMap<String,String> config= new HashMap<String,String>()
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 'http://localhost:8081')
        jacksonSerDe.configure( config  ,false)

        return new KafkaProducer<>(props,Serdes.String().serializer(),jacksonSerDe.serializer())
//        return new KafkaProducer<String, String>(props)
    }


    @CompileStatic
    public static Consumer<String, User> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 'localhost:9092')
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 'http://localhost:8081')
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, 'testClient');
        props.put(ConsumerConfig.GROUP_ID_CONFIG,'testGroup')
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        AvroMapper mapper = new AvroMapper()
        mapper.registerModule(new JavaTimeModule())
        JacksonAvroSerDe<User> jacksonSerDe= new JacksonAvroSerDe<User>(User.class, mapper)

        HashMap<String,String> config= new HashMap<String,String>()
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 'http://localhost:8081')
        jacksonSerDe.configure( config  ,false)

        KafkaConsumer<String,User> consumer= new KafkaConsumer<String,User>(props,Serdes.String().deserializer(),jacksonSerDe.deserializer())
        consumer.subscribe(['testtopic'])
        return consumer
    }

}
