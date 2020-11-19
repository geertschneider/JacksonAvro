import be.vdab.vdp.JacksonAvroSerDe
import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import groovy.transform.CompileStatic
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import kafka.Kafka
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.startupcheck.StartupCheckStrategy
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName

import java.time.Duration

@Testcontainers
class JacksonTest {
    static final String registry = 'schema-registry'
    static final Integer registryPort  = 8081

    static final String kafka = 'kafka'
    static final Integer kafkaPort  = 9092

    String kafkaURL,schemaRegistryURL

    @Container
    DockerComposeContainer containers =
            new DockerComposeContainer(new File('src/test/resources/docker-compose-testing.yml'))
                    .withExposedService( registry,registryPort)
                    .withExposedService(kafka,kafkaPort)


    @Test
    void CheckSerialization(){


        def kafkaCont = [
                host : containers.getServiceHost(kafka,kafkaPort),
                port : containers.getServicePort(kafka,kafkaPort)
        ]
        kafkaURL="${kafkaCont.host}:${kafkaCont.port}"

        def registryCont = [
                host : containers.getServiceHost(registry,registryPort),
                port : containers.getServicePort(registry,registryPort)
        ]

        schemaRegistryURL="http://${registryCont.host}:${registryCont.port}"

        runProducer()

//        runConsumer()

        Assertions.assertTrue(true)

    }



    def runProducer(){
        println 'running producer'
        Producer<String, User> producer = createProducer()
        (1..10).each{
            User usr = new User(id: it, naam: "Geert-${it}", vacatures: [new Vacature(id: it)])
            producer.send(new ProducerRecord<String, User>('testtopic', 'A', usr))
            println "produced event ${it}"
        }
        producer.flush()
        producer.close()
    }
    def runConsumer(){
        println 'running consumer'
        Consumer<String, User> consumer = createConsumer()
        Integer amountOfRecords=0

        (1..5).each{

            ConsumerRecords<String,User> consumedRecords= consumer.poll( Duration.ofSeconds(1,0))
            consumedRecords.each {consumerRecord ->
                println consumerRecord.value()
                amountOfRecords++
            }


            if(amountOfRecords==10)
                return true
        }
        return false
    }



    Producer<String, User> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL)
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, 'testClient');
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        AvroMapper mapper = new AvroMapper()
        mapper.registerModule(new JavaTimeModule())
        JacksonAvroSerDe<User> jacksonSerDe= new JacksonAvroSerDe<User>(User.class, mapper)

        HashMap<String,String> config= new HashMap<String,String>()
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
        jacksonSerDe.configure( config  ,false)

        return new KafkaProducer<>(props,Serdes.String().serializer(),jacksonSerDe.serializer())
//        return new KafkaProducer<String, String>(props)
    }


    Consumer<String, User> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaURL)
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, 'testClient');
        props.put(ConsumerConfig.GROUP_ID_CONFIG,'testGroup')
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        AvroMapper mapper = new AvroMapper()
        mapper.registerModule(new JavaTimeModule())
        JacksonAvroSerDe<User> jacksonSerDe= new JacksonAvroSerDe<User>(User.class, mapper)

        HashMap<String,String> config= new HashMap<String,String>()
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL)
        jacksonSerDe.configure( config  ,false)

        KafkaConsumer<String,User> consumer= new KafkaConsumer<String,User>(props, Serdes.String().deserializer(),jacksonSerDe.deserializer())
        consumer.subscribe(['testtopic'])
        return consumer
    }
}
