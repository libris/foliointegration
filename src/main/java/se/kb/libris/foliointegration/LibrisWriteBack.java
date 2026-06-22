package se.kb.libris.foliointegration;

import com.fasterxml.jackson.databind.JsonNode;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
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

    static String LIBRIS_BASE_URL = System.getenv("LIBRIS_BASE_URL");

    final static Properties props = new Properties() {{
        String sslCert = System.getenv("FOLIO_KAFKA_CLIENT_CERT");
        String bootstrapServers = System.getenv("FOLIO_KAFKA_SERVERS");

        put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        put(GROUP_ID_CONFIG, "libris-integration"); // Must be a "unique" name. If shared with something in folio, we would "steal" events that they would miss.
        put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        if (sslCert != null) {
            put(SECURITY_PROTOCOL_CONFIG, "SSL");
            put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, sslCert);
            put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        }
    }};
    final static Consumer<String, String> consumer = new KafkaConsumer<>(props);
    final static String topic = "folio.ALL.inventory.item";
    static {
        consumer.subscribe(Arrays.asList(topic));
        // TODO: Consumer.seek(long) // to our last handled timeStamp. No? Maybe no need.
    }

    public static boolean run() throws IOException, ParseException {
        boolean changed = false;
        String librisAuthToken = getAuthToken();
        if (librisAuthToken != null) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                //Storage.log(String.format("Kafka event %s: key = %-10s value = %s", topic, key, value));
                changed |= handleEvent(value, librisAuthToken);
            }
        }

        return changed;
    }

    private static boolean handleEvent(String event, String librisAuthToken) {
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
            String[] librisHoldingRecordAndEtag = doLibrisGet(new URI(librisHoldingUri));
            Map librisHoldingMap = Storage.mapper.readValue(librisHoldingRecordAndEtag[0], Map.class);

            // Apply the JSLT transform to our folio holding and items, to get a libris component-list
            Expression writebackJSLT = Parser.compileString(Format.librisWritebackJsltConversion, new ArrayList<>()); // no extra functions for now.
            JsonNode originalJsonNode = Storage.mapper.valueToTree(holdingMap);
            JsonNode transformedJsonNode = writebackJSLT.apply(originalJsonNode);
            List newLibrisComponentList = Storage.mapper.treeToValue(transformedJsonNode, List.class);

            // Set the proper ShelfMark and shelfControlNumber on each item.
            for (Object o : newLibrisComponentList) {
                if (o instanceof Map m) {

                    // .shelfMark = Sv2023Q | Sv2023Q 346346

                    // slå upp Sv2023Q och länka
                    // sätt direkt på exemplar:
                    // shelfControlNumber	"1815"
                    // shelfMark            [0] {@id : länktillSC2023...}

                    // If an item has a multi-word shelfMark, the first is the "suite" (sequence) the rest is the number from that sequence.
                    // The sequence must be replaced by a link in libris.
                    // If there was no sequence number, one must be taken from the sequence.

                    Storage.log(" ** HANDLING AN INCOMING HOLD: " + Storage.mapper.writeValueAsString(m));

                    if (m.containsKey("shelfMark")) {
                        Object sm = m.get("shelfMark");
                        if (sm instanceof List l) {
                            sm = l.get(0);
                        }
                        Map shelfMarkMap = ((Map) sm);
                        Object label = shelfMarkMap.get("label");
                        if (label instanceof List l) {
                            label = l.get(0);
                        }
                        String shelfMarkString = ( (String) label ).trim();

                        Storage.log("    HANDLING AN INCOMING SHELFMARK: " + shelfMarkString);

                        int spaceAt = shelfMarkString.indexOf(" ");
                        String sequenceString;
                        String controlNumberString;
                        if (spaceAt != -1) {
                            sequenceString = shelfMarkString.substring(0, spaceAt).trim();
                            controlNumberString = shelfMarkString.substring(spaceAt).trim();
                        } else {
                            sequenceString = shelfMarkString;
                            controlNumberString = null;
                        }

                        Storage.log("    parsed as : " + sequenceString + " / " + controlNumberString);

                        String sequenceUri = lookupShelfMarkSequence(sequenceString);

                        Storage.log("    looked up as : " + sequenceString + " = " + sequenceUri);

                        if (sequenceUri != null) { // Link a found shelfMark sequence.
                            m.put("shelfMark", List.of(Map.of("@id", sequenceUri)));
                            if (controlNumberString == null) { // There appears to be only a "signum svit" but no sequence number here
                                controlNumberString = reserveShelfControlNumber(sequenceUri);
                                m.put("shelfControlNumber", controlNumberString);
                            }
                        }
                        else {
                            Storage.log("Found no shelf mark sequence for: " + sequenceString + " in libris. Leaving shelf mark as is.");
                        }
                    }

                }
            }

            // Replace the old hasComponent list with the new one.
            List graphList = (List) librisHoldingMap.get("@graph");
            while (graphList.size() > 2) { // we don't want "lens cards" and such crap, just the plain data for this record.
                graphList.removeLast();
            }
            Map mainEntity = (Map) graphList.get(1);
            mainEntity.put("hasComponent", newLibrisComponentList);
            librisHoldingMap.remove("@context");

            // Write to LIBRIS
            int writeResultCode = 0;
            do {
                writeResultCode = writeLibrisHolding(librisHoldingUri, librisHoldingMap, librisHoldingRecordAndEtag[1], librisAuthToken, sigel);
            } while (writeResultCode == 429); // If we get a 429 (concurrent modification) try again.

            return true;

        } catch (Exception e) {
            Storage.log("Failed handling KAFKA event. The value received from KAFKA was this:\n" + event, e);
        }
        return false;
    }

    private static String reserveShelfControlNumber(String sequenceUri) {
        return "100 TEST!";
    }

    private static String lookupShelfMarkSequence(String name) throws URISyntaxException, IOException, ProtocolException {
        URI findUri = new URI(LIBRIS_BASE_URL);
        String[] result = doLibrisGet(findUri.resolve("/find?_q=type:ShelfMarkSequence%20" + name));
        if (result[2].equals("200")) {
            Map searchResultMap = Storage.mapper.readValue(result[0], Map.class);
            if (searchResultMap.containsKey("items")) {
                for (Object o : (List)searchResultMap.get("items")) {
                    if (o instanceof Map item) {
                        if (item.containsKey("label")) {
                            Object ol = item.get("label");
                            if (ol instanceof String labelString) {
                                if (labelString.equals(name)) {
                                    return (String) item.get("@id");
                                }
                            } else if (ol instanceof List l) {
                                if (l.size() > 0 && l.get(0).equals(name)) {
                                    return (String) item.get("@id");
                                }
                            }
                        }
                    }
                }
            }
        }
        //throw new RuntimeException("Failed ShelfMarkSequence lookup on " + name);
        return null;
    }

    // Returns response (0) and ETAG (1) (or throws)
    private static String[] doLibrisGet(URI uri) throws IOException, ProtocolException {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

            HttpGet request = new HttpGet(uri);
            RequestConfig config = RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
            request.setConfig(config);

            request.setHeader("Accept", "application/ld+json");
            request.setHeader("User-Agent", "FOLIO integration");
            ClassicHttpResponse response = httpClient.execute(request);

            Header etagHeader =  response.getHeader("ETag");
            String etag = null;
            if (etagHeader != null)
                etag = etagHeader.getValue();

            String responseText = EntityUtils.toString(response.getEntity());
            String[] tuple = {responseText, etag, ""+response.getCode()};
            return tuple;
        }
    }

    // Turn FOLIO GUIDS (where we know what they mean) back into meaningful strings.
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

    /*
    private static String getUriFromLibris(String lookupCode, String key) throws IOException, URISyntaxException, ProtocolException {


        if (lookupCode.equals("__LIBRIS_LOOKUP_SHELFMARK")) {
            // Do type:(ShelfMarkSequence) Sv2023Q
            // type:(ShelfMarkSequence) + key

            // Find single/best, return.

            URI findUri = new URI(LIBRIS_BASE_URL);
            String[] result = doLibrisGet(findUri.resolve("/find?_q=type:ShelfMarkSequence " + key));
            if (result[2].equals("200")) {
                System.err.println("** RESULT OF SEARCHING FOR " + key + " :\n" + result[0]);
            }


            //findHoldUri = findHoldUri.resolve("/_findhold?library=" + libraryUri + "&id=" + librisInstanceUri);
            //                String[] librisHoldingUriListAndEtag = doLibrisGet(findHoldUri);
        }

        return null;
    }

    private static String librisLookup(Object node, String JSLTKey) throws IOException, URISyntaxException, ProtocolException {
        if (node instanceof Map) {
            Map map = (Map) node;

            if (map.containsKey(JSLTKey)) {
                return getUriFromLibris( JSLTKey, (String) map.get(JSLTKey) );
            }

            String removedKey = null;
            String guid = null;
            Iterator it = map.keySet().iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                String result = librisLookup(map.get(key), JSLTKey);
                if (result != null) {
                    guid = result;
                    it.remove();
                    removedKey = key;
                }
            }
            if (guid != null) {
                map.put(removedKey, guid);
            }

        } else if (node instanceof List) {
            List list = (List) node;
            String guid = null;
            Iterator it =  list.iterator();
            while (it.hasNext()) {
                Object element = it.next();

                String result = librisLookup(element, JSLTKey);
                if (result != null) {
                    guid = result;
                    it.remove();
                }
            }
            if (guid != null) {
                list.add(guid);
            }
        }

        return null;
    }*/

    public static String getAuthToken() throws IOException, ParseException {
        final String loginUrl = System.getenv("LIBRIS_LOGIN_URL");
        final String clientId = System.getenv("LIBRIS_CLIENT_ID");
        final String clientSecret = System.getenv("LIBRIS_CLIENT_SECRET");

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

            HttpPost request = new HttpPost(loginUrl);
            RequestConfig config = RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
            request.setConfig(config);

            request.setHeader("Accept", "application/json");
            request.setHeader("User-Agent", "FOLIO integration");

            final List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("client_id", clientId));
            params.add(new BasicNameValuePair("client_secret", clientSecret));
            params.add(new BasicNameValuePair("grant_type", "client_credentials"));
            request.setEntity(new UrlEncodedFormEntity(params));

            ClassicHttpResponse response = httpClient.execute(request);
            if (response.getCode() == 200) {
                String responseText = EntityUtils.toString(response.getEntity());
                Map responseMap = Storage.mapper.readValue(responseText, Map.class);
                String token = (String) responseMap.get("access_token");
                return token;
            } else {
                Storage.log("Failed retrieving authentication token from " + loginUrl + " got: " + response.getCode() + " with message: " + EntityUtils.toString(response.getEntity()));
                return null;
            }
        }
    }

    public static int writeLibrisHolding(String librisHoldingUri, Map librisHolding, String ETag, String authToken, String sigel) throws IOException, ParseException {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

            HttpPut request = new HttpPut(librisHoldingUri);
            RequestConfig config = RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
            request.setConfig(config);

            request.setHeader("Content-Type", "application/ld+json");
            request.setHeader("If-Match", ETag);
            request.setHeader("User-Agent", "FOLIO integration");
            request.setHeader("XL-Active-Sigel", sigel);
            request.setHeader("Authorization", authToken);

            request.setEntity(new StringEntity(Storage.mapper.writeValueAsString(librisHolding)));

            ClassicHttpResponse response = httpClient.execute(request);
            if (response.getCode() == 204) {
                Storage.log("Wrote to " + librisHoldingUri);
            }
            else if (response.getCode() != 429){
                Storage.log("Failed writing " + librisHoldingUri + " to LIBRIS, got: " + response.getCode() + " with message: " + EntityUtils.toString(response.getEntity()));
            }
            return response.getCode();
        }
    }
}
