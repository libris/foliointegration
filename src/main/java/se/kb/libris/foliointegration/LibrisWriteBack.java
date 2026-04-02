package se.kb.libris.foliointegration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class LibrisWriteBack {

    public static boolean run() {

        final Properties props = new Properties() {{
            String sslCert = System.getenv("FOLIO_KAFKA_CLIENT_CERT");
            String bootstrapServers = System.getenv("FOLIO_KAFKA_SERVERS");

            put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getCanonicalName());
            put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(GROUP_ID_CONFIG,                 "libris-integration");
            put(AUTO_OFFSET_RESET_CONFIG,        "earliest");

            put(SECURITY_PROTOCOL_CONFIG,        "SSL");
            put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, sslCert);
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        }};

        final String topic = "folio.ALL.inventory.instance";

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    Storage.log(String.format("Kafka event %s: key = %-10s value = %s", topic, key, value));
                }
            }
        }
    }
}
