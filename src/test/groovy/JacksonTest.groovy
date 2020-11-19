import kafka.Kafka
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

@Testcontainers
class JacksonTest {
    static final String registry = 'schema-registry'
    static final String kafka = 'kafka'
    static final Integer kafkaPort  = 9092
    static final Integer registryPort  = 8081

    @Container
    DockerComposeContainer containers =
            new DockerComposeContainer(new File('src/test/resources/docker-compose-testing.yml'))
                    .withExposedService( registry,registryPort)
                    .withExposedService(kafka,kafkaPort)

//    @Container
//    GenericContainer zookeeperCont=new GenericContainer(DockerImageName.parse('confluentinc/cp-zookeeper:5.5.0'))

//    @Container
//    GenericContainer kafkaCont=new GenericContainer(DockerImageName.parse('confluentinc/cp-zookeeper:5.5.0'))
//            .withEnv(
//                    [
//                            KAFKA_ZOOKEEPER_CONNECT:'zookeeper:2181',
//                            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
//                            KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
//                            KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092',
//                            KAFKA_AUTO_CREATE_TOPICS_ENABLE:'true'
//                    ]
//            ).withExposedPorts(9092)
//            .dependsOn([zookeeperCont])
//    @Container
//    KafkaContainer kafkaCont=new KafkaContainer(DockerImageName.parse('confluentinc/cp-kafka:5.5.0')).withExternalZookeeper('zookeeper:2181').dependsOn([zookeeperCont])
////
//    @Container
//    GenericContainer schemaRegistryCont=new GenericContainer(DockerImageName.parse('confluentinc/cp-zookeeper:5.5.0'))
//            .withEnv(
//                    [
//                            SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL: 'zookeeper:2181'
//                    ]
//            )
//            .dependsOn([zookeeperCont,kafkaCont])



    @Test
    void CheckKafka(){
println('containers started')

    }

//    @Test
//    void CheckKafkaRuns(){
//
//
//        containers.waitingFor(registry,Wait.forHttp().forPort(registryPort))
//        containers.waitingFor(kafka,Wait.forHealthcheck())
//
//
////
////        Integer schemaPort = containers.getServicePort(registry,8081)
////        Integer kafkaPort = containers.getServicePort('kafka',9092)
////
////        containers.waitingFor('schema-registry', Wait.forHttp(schemaPort) )
//        String host= containers.getServiceHost(kafka,kafkaPort)
//        def port= containers.getServicePort(kafka,kafkaPort)
//        println "kafka  running at ${host} - ${port}"
//        Assertions.assertTrue(true)
//    }
}
