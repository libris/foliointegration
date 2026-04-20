package se.kb.libris.foliointegration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class LibrisWriteBack {

    static String temporaryJslt = """
            let root = (.) // root is the folio holding record
            
            [
              // In the holding record, "items" (the FOLIO items) have been artificially added.
              // Wherever a GUID is found (if not under {"id": ...}) the GUID will have been replaced
              // by whatever we've previously mapped that GUID to, if anything (for transforming in the other direction)
              // The result of this must be a new hasComponent list for the Libris holding record:
              for (.items)
              {
                "hasNote": {"@type": "Note", "label": $root.permanentLocationId},
                "shelfMark" : {"@type": "ShelfMark", "label": .barcode }
              }
            ]
            
            """;

    public static boolean run() {

        final Properties props = new Properties() {{
            String sslCert = System.getenv("FOLIO_KAFKA_CLIENT_CERT");
                String bootstrapServers = System.getenv("FOLIO_KAFKA_SERVERS");

            put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(GROUP_ID_CONFIG, "libris-integration"); // Must be a "unique" name. If shared with something in folio, we would "steal" events that they would miss.
            put(AUTO_OFFSET_RESET_CONFIG, "earliest");

            put(SECURITY_PROTOCOL_CONFIG, "SSL");
            put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, sslCert);
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        }};

        final String topic = "folio.ALL.inventory.item";

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));

            // TODO: Consumer.seek(long) // to our last handled timeStamp

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    Storage.log(String.format("Kafka event %s: key = %-10s value = %s", topic, key, value));
                    handleEvent(value);
                }
            }
        }
    }

    private static void handleEvent(String event) {
        try {
            Map eventMap = Storage.mapper.readValue(event, Map.class);
            Long eventTimeStamp = (Long) eventMap.get("eventTs");

            // Both creations and edits have "new" (updates also have "old").
            Map item = (Map) eventMap.get("new");
            String holdingId = (String) item.get("holdingsRecordId");
            String holdingString = FolioWriting.getFromFolio("/holdings-storage/holdings/" + holdingId);
            //Storage.log(" **** fetched holding: " + holdingString);


            Map holdingMap = Storage.mapper.readValue(holdingString, Map.class);

            String itemsString = FolioWriting.getFromFolio("/inventory/items-by-holdings-id?offset=0&limit=2000&query=holdingsRecordId=" + holdingId);
            //Storage.log(" **** fetched items for holding: " + holdingId + ":\n" + itemsString);

            Map itemsMap = Storage.mapper.readValue(itemsString, Map.class);
            holdingMap.put("items", itemsMap.get("items"));

            doReverseLookups(holdingMap);

            Storage.log(" **** ready for transfrom for: " + holdingId + ":\n" + Storage.mapper.writeValueAsString(holdingMap));

            Expression writebackJSLT = Parser.compileString(temporaryJslt, new ArrayList<>()); // no extra functions for now.
            JsonNode originalJsonNode = Storage.mapper.valueToTree(holdingMap);
            JsonNode transformedJsonNode = writebackJSLT.apply(originalJsonNode);
            List librisComponentList = Storage.mapper.treeToValue(transformedJsonNode, List.class);

            Storage.log(" **** Transformed component list for : " + holdingId + ":\n" + Storage.mapper.writeValueAsString(librisComponentList));

        } catch (Exception e) {
            Storage.log("Failed handling KAFKA event. The value received from KAFKA was this:\n" + event, e);
        }
    }

    // Turn GUIDS (where we know what they mean) back into meaningful strings.
    private static void doReverseLookups(Object folioData)
    {
        if (folioData instanceof Map m) {
            for (Object key : m.keySet()) {
                if (!key.equals("id")) {

                    if (m.get(key) instanceof String s) {
                        if (Format.guidReverseLookup.containsKey(s)) {
                            m.put(key, Format.guidReverseLookup.get(s));
                        }
                    }

                    doReverseLookups(m.get(key));
                }
            }
        } else if(folioData instanceof List l) {
            for (Object o : l) {
                doReverseLookups(o);
            }
        }
    }
}
