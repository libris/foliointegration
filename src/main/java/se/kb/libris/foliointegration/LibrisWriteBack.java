package se.kb.libris.foliointegration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class LibrisWriteBack {

    /*static String temporaryJslt = """
            let root = (.) // root is the folio holding record

            [
                 // In the holding record, "items" (the FOLIO items) have been artificially added.
                 // The "librisLibraryUri"-property is also artificially added.
                 // Wherever a GUID is found (if not under {"id": ...}) the GUID will have been replaced
                 // by whatever we've previously mapped that GUID to, if anything (for transforming in the other direction).
                 // The result of this transform must be a new hasComponent list for the Libris holding record:
                 for (.items) {
                     "heldBy": {"@id": $root.librisLibraryUri},
                     "hasNote": {"@type": "Note", "label": $root.permanentLocationId},
                     "shelfMark" : {"@type": "ShelfMark", "label": .barcode }
                 }
            ]
            
            
            """;*/

    static String LIBRIS_BASE_URL = System.getenv("LIBRIS_BASE_URL");

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

            // Both creations and edits have "new" (updates also have "old"). Get the folio holding
            Map item = (Map) eventMap.get("new");
            String holdingId = (String) item.get("holdingsRecordId");
            String holdingString = FolioWriting.getFromFolio("/holdings-storage/holdings/" + holdingId);
            Map holdingMap = Storage.mapper.readValue(holdingString, Map.class);

            // Get the folio instance
            String instanceId = (String) holdingMap.get("instanceId");
            String instanceString = FolioWriting.getFromFolio("/instance-storage/instances/" + instanceId);
            Map instanceMap = Storage.mapper.readValue(instanceString, Map.class);
            String librisInstanceUri = (String) instanceMap.get("sourceUri");

            // Get all the items for this holding, and "put them in" the holding
            String itemsString = FolioWriting.getFromFolio("/inventory/items-by-holdings-id?offset=0&limit=2000&query=holdingsRecordId=" + holdingId);
            Map itemsMap = Storage.mapper.readValue(itemsString, Map.class);
            holdingMap.put("items", itemsMap.get("items"));
            doReverseLookups(holdingMap);

            // Figure out which libris library this is about
            // TEMP, ASSUMPTIONS ABOUT KB SIGEL BASED ON LOCATION. THIS SHOULD NOT REMAIN
            String sigel = "S";
            if (holdingMap.get("permanentLocationId").equals("Rogge"))
                sigel = "SRo";
            // Library URIs are *not* env-specific..
            //String libraryUri = new URI(LIBRIS_BASE_URL).resolve("/library/"+sigel).toString();
            String libraryUri = new URI("https://libris.kb.se/library/"+sigel).toString();
            holdingMap.put("librisLibraryUri", libraryUri);

            // Having a libris bibliographic URI + Library URI means we can identify the libris holding record in question.
            URI findHoldUri = new URI(LIBRIS_BASE_URL);
            findHoldUri = findHoldUri.resolve("/_findhold?library=" + libraryUri + "&id=" + librisInstanceUri);
            String[] librisHoldingUriListAndEtag = doLibrisGet(findHoldUri);
            List librisHoldingUriList = Storage.mapper.readValue(librisHoldingUriListAndEtag[0], List.class);
            if (librisHoldingUriList.isEmpty())
                throw new RuntimeException("Unable to locate libris holding record for instance: " + librisInstanceUri + " and library " + libraryUri);
            String librisHoldingUri = (String) librisHoldingUriList.get(0);
            String[] librisHoldingRecordAndEtag = doLibrisGet( new URI(librisHoldingUri) );
            Map librisHoldingMap = Storage.mapper.readValue(librisHoldingRecordAndEtag[0], Map.class);
            //Storage.log("Libris holding: " + librisHoldingRecordAndEtag[0] + "\nETAG: " + librisHoldingRecordAndEtag[1]);

            // Apply the JSLT transform to our folio holding and items, to get a libris component-list
            //Storage.log(" **** ready for transform for: " + holdingId + ":\n" + Storage.mapper.writeValueAsString(holdingMap));
            Expression writebackJSLT = Parser.compileString(Format.librisWritebackJsltConversion, new ArrayList<>()); // no extra functions for now.
            JsonNode originalJsonNode = Storage.mapper.valueToTree(holdingMap);
            JsonNode transformedJsonNode = writebackJSLT.apply(originalJsonNode);
            List newLibrisComponentList = Storage.mapper.treeToValue(transformedJsonNode, List.class);
            //Storage.log(" **** Transformed component list for : " + holdingId + ":\n" + Storage.mapper.writeValueAsString(newLibrisComponentList));

            // Replace the old hasComponent list with the new one.

            List graphList = (List) librisHoldingMap.get("@graph");
            while (graphList.size() > 2) { // we don't want "lens cards" and such crap, just the plain data for this record.
                graphList.removeLast();
            }
            Map mainEntity = (Map) graphList.get(1);
            mainEntity.put("hasComponent", newLibrisComponentList);
            librisHoldingMap.remove("@context");
            Storage.log("  ** Ready to write to libris?: " + Storage.mapper.writeValueAsString(librisHoldingMap));

        } catch (Exception e) {
            Storage.log("Failed handling KAFKA event. The value received from KAFKA was this:\n" + event, e);
        }
    }

    // Returns response (0) and ETAG (1) (or throws)
    private static String[] doLibrisGet(URI uri) throws IOException, ProtocolException {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

            HttpGet request = new HttpGet(uri);
            RequestConfig config = RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
            request.setConfig(config);

            request.setHeader("Accept", "application/ld+json");
            ClassicHttpResponse response = httpClient.execute(request);

            /*for (Header h : response.getHeaders()) {
                Storage.log("*** HEADER ?? : " +h);
            }*/
            Header etagHeader =  response.getHeader("ETag");
            String etag = null;
            if (etagHeader != null)
                etag = etagHeader.getValue();

            // Libris ETags are weak (start with "W/"), this might have to be stripped out before sending back.

            String responseText = EntityUtils.toString(response.getEntity());
            String[] tuple = {responseText, etag};
            return tuple;
        }
    }

    // Turn GUIDS (where we know what they mean) back into meaningful strings.
    private static void doReverseLookups(Object folioData)
    {
        if (folioData instanceof Map m) {
            // This is surprisingly OK! One would think it would throw a ConcurrentModification thingy?
            // But is actually OK since the keyset remains unchanged (only the value is replaced). TIL.
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
            Iterator it = l.iterator();
            List<String> toBeAdded = new ArrayList<>(1);
            while(it.hasNext()) {
                Object o = it.next();
                if (o instanceof String s && Format.guidReverseLookup.containsKey(s)) {
                    it.remove();
                    toBeAdded.add(Format.guidReverseLookup.get(s));
                } else {
                    doReverseLookups(o);
                }
            }
            l.addAll(toBeAdded);
        }
    }
}
