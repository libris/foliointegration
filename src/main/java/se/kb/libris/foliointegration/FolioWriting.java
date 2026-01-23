package se.kb.libris.foliointegration;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class FolioWriting {
    private static final String username;
    private static final String password;
    private static final String folioBaseUri;
    private static final String folioTenant;
    private static final int folioWriteBatchSize;
    private static final int folioBatchesPerCell;
    private static final long folioCellSeconds;

    private static List<Map> batch = new ArrayList<>();
    private static List<Thread> hridLookupThreads = new ArrayList<>();

    private static long folioTokenValidUntil = 0;
    private static String folioToken = null;

    // Generic Cell Rate Algorithm (GCRA)
    // Simple/effective way to do throttling.
    private static Instant throttlingCell = Instant.now();
    private static int throttlingCount = 0;

    static {
        username = System.getenv("FOLIO_USER");
        password = System.getenv("FOLIO_PASS");
        folioBaseUri = System.getenv("OKAPI_URL");
        folioTenant = System.getenv("OKAPI_TENANT");

        folioWriteBatchSize = Integer.parseInt( System.getenv("FOLIO_WRITE_BATCH_SIZE") );
        folioBatchesPerCell = Integer.parseInt( System.getenv("FOLIO_WRITE_BATCHES_PER_CELL") );
        folioCellSeconds = Long.parseLong( System.getenv("FOLIO_WRITE_CELL_SECONDS") );
    }

    private static String getToken() {

        // This will seem weird, but we want key handling to be synchronized,
        // but SEPARATELY from the below queue/flush calls (which synchronize over the class)
        synchronized (password) {
            if (System.currentTimeMillis() < folioTokenValidUntil)
                return folioToken;

            for (int i = 0; i < 10; ++i) {
                try {
                    URI uri = new URI(folioBaseUri);
                    uri = uri.resolve("/authn/login-with-expiry");
                    var requestBodyMap = Map.of("tenant", folioTenant, "username", username, "password", password);
                    String requestBody = Storage.mapper.writeValueAsString(requestBodyMap);

                    HttpPost request = new HttpPost(uri);
                    RequestConfig config = RequestConfig.custom()
                            .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                    request.setConfig(config);
                    request.setHeader("X-Okapi-Tenant", folioTenant);
                    request.setHeader("Accept", "application/json");
                    request.setHeader("Content-type", "application/json");

                    StringEntity entity = new StringEntity(requestBody);
                    request.setEntity(entity);

                    ClassicHttpResponse response = Server.httpClient.execute(request);
                    Header[] headers = response.getHeaders();
                    for (Header header : headers) {
                        if (header.getValue().startsWith("folioAccessToken")) {

                            // We will need the token max age.
                            String[] parts = header.getValue().split(";"); // Separate cookie parts
                            for (int j = 0; j < parts.length; ++j) {
                                if (parts[j].trim().startsWith("Max-Age=")) {
                                    String maxAgeSeconds = parts[j].substring(9);
                                    folioTokenValidUntil = Long.parseLong(maxAgeSeconds) * 1000 + System.currentTimeMillis() - 60000; // Keep a 1 minute margin
                                }
                            }

                            folioToken = header.getValue();
                            return folioToken;
                        }
                    }

                    Storage.log("Unexpected FOLIO login response: " + EntityUtils.toString(response.getEntity()) + " / " + Arrays.toString(response.getHeaders()));
                } catch (IOException | URISyntaxException | ProtocolException e) {
                    Storage.log("No token.", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e2) {
                        // ignore
                    }
                }
            }

            return null;
        }
    }

    public static String getFromFolio(String pathAndParameters) throws IOException {
        String token = getToken();

        for (int i = 0; i < 20; ++i) {
            try {

                URI uri = new URI(folioBaseUri);
                uri = uri.resolve(pathAndParameters);
                HttpGet request = new HttpGet(uri);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);

                request.setHeader("X-Okapi-Tenant", folioTenant);
                request.setHeader("Accept", "application/json");
                request.setHeader("Cookie", token);

                ClassicHttpResponse response = Server.httpClient.execute(request);
                String responseText = EntityUtils.toString(response.getEntity());

                if (response.getCode() != 200) {
                    Storage.log("Failed FOLIO lookup: " + response);
                    return null;
                }
                return responseText;
            } catch (IOException | URISyntaxException | ParseException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    // ignore
                }
                Storage.log("Retrying Folio lookup for: " + pathAndParameters);
            }
        }
        throw new IOException("Unable to complete request: " + pathAndParameters);
    }

    private static void lookupFolioHRID(Map folioRecord) {

        // Have to retry forever, because the alternative (to not getting a definitive answer)
        // is creating a double record.
        while (true) {

            try {
                Map instanceToBeSent = (Map) folioRecord.get("instance");
                String mainEntityUri = (String) instanceToBeSent.get("sourceUri");
                String pathAndParameters = "/inventory/instances?query=sourceUri==" + URLEncoder.encode("\"" + mainEntityUri + "\"", StandardCharsets.UTF_8);
                String response = getFromFolio(pathAndParameters);

                if (response == null)
                    continue;

                Map responseMap = Storage.mapper.readValue(response, Map.class);
                if (responseMap.containsKey("instances")) {
                    List instances = (List) responseMap.get("instances");
                    if (!instances.isEmpty()) {
                        Map instanceFromFolio = (Map) instances.get(0); // There should never be more than one instance having this specific ID
                        if (instanceFromFolio.containsKey("hrid")) {
                            //Storage.log("Replaced outgoing HRID: " + instanceFromFolio.get("hrid"));
                            instanceToBeSent.put("hrid", instanceFromFolio.get("hrid"));
                            return;
                        }
                    } else {
                        // NO HRID OBATINED FROM FOLIO, LEAVE THE CONTROLNUMBER AS IS!
                        Storage.log("Minting a new folio ID: " + instanceToBeSent.get("hrid"));
                        return;
                    }
                }
            } catch (IOException ioe) {
                Storage.log("Failed HRID lookup, will retry later.", ioe);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
            System.err.println("Retrying HRID...");
        }
    }

    public static synchronized void queueForExport(Map _folioRecord, Connection connection) throws IOException, InterruptedException, SQLException {
        HashMap folioRecord = new HashMap(_folioRecord);
        batch.add(folioRecord);

        // We no longer need to do this, since redefining folio HRIDS as Libris control numbers.
        //Thread t = Thread.startVirtualThread(() -> lookupFolioHRID(folioRecord));
        //hridLookupThreads.add(t);

        if (batch.size() >= folioWriteBatchSize) { // Too large batches results in internal http 414 in folio.
            flushQueue(connection);
        }
    }

    public static synchronized void flushQueue(Connection connection) throws IOException, InterruptedException, SQLException {

        // All HRID lookups must have concluded before flushing is possible
        for (Thread t : hridLookupThreads) {
            t.join();
        }
        hridLookupThreads.clear();

        if (batch.isEmpty())
            return;


        // TEMP: DO NOT ACTUALLY WRITE ANYTHING!
        /*if (1 == 1) {
            List<String> writtenIDs = new ArrayList<>();
            for (Map record : batch) {
                writtenIDs.add( (String) ((Map)record.get("instance")).get("hrid") );
            }
            Storage.log("[WOULD HAVE] Written (but not live) " + batch.size() + " records to FOLIO: " + writtenIDs);
            batch.clear();
            return;
        }*/
        // REMOVE THIS

        Map recordSet = Map.of("inventoryRecordSets", batch);
        String body = Storage.mapper.writeValueAsString(recordSet);


        try {

            // Throttle if necessary
            while (true) {
                Instant now = Instant.now();
                if (now.isAfter( throttlingCell.plus(folioCellSeconds, ChronoUnit.SECONDS) ) ) {
                    throttlingCell = now;
                    throttlingCount = 0;
                }
                int cellCount = ++throttlingCount;
                if (cellCount <= folioBatchesPerCell) {
                    // Ok to write now!
                    break;
                }
                else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ie) { /* ignore */ }
                }
            }

            String token = getToken();

            URI uri = new URI(folioBaseUri);
            uri = uri.resolve("/inventory-batch-upsert-hrid");
            HttpPut request = new HttpPut(uri);
            RequestConfig config = RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
            request.setConfig(config);

            StringEntity entity = new StringEntity(body);
            request.setEntity(entity);

            request.setHeader("X-Okapi-Tenant", folioTenant);
            request.setHeader("Accept", "application/json");
            request.setHeader("Content-type", "application/json");
            request.setHeader("Cookie", token);

            ClassicHttpResponse response = Server.httpClient.execute(request);
            String responseText = EntityUtils.toString(response.getEntity());

            // These two use the same indexing. Meaning errorShortMessagesInBatch[5] refers to the message received for failedHridsInBatch[5]
            List<String> failedHridsInBatch = new ArrayList<>();
            List<String> errorMessagesInBatch = new ArrayList<>();

            if (response.getCode() == 207) { // "Multi-status", mixed response. We need to figure out which records went bad
                // Need to parse error message per record tried: /errors/N/entity/hrid
                // If folio changes the way it reports errors, this will break down in a hurry.
                // If that ever happens, the easy temporary fix, is to ignore this code, and just set
                // the batch size to 1.
                Map responseMap = Storage.mapper.readValue(responseText, Map.class);
                if (responseMap.containsKey("errors")) {
                    if ( responseMap.get("errors") instanceof List errors) {
                        for (Object o : errors) {
                            if ( o instanceof Map error) {
                                if ( error.get("entity") instanceof Map requesEntity) {
                                    if ( requesEntity.get("hrid") instanceof String hridBroken) {
                                        failedHridsInBatch.add(hridBroken);
                                    }
                                }
                                if ( error.get("message") instanceof String message) {
                                    errorMessagesInBatch.add(message);
                                } else {
                                    errorMessagesInBatch.add("No message in error response.");
                                }
                            }
                        }
                    }
                }

            } else if (response.getCode() != 200) {

                // If neither 200 nor 207, something else (unknown) has happened. This could be things like network problems,
                // downtimes, or something else entirely. We cannot proceed without a retry.

                Storage.log("Failed FOLIO write: " + response + " / " + responseText);
                return;
            }

            // IF OK
            List<String> writtenIDs = new ArrayList<>();
            for (Map record : batch) {
                writtenIDs.add( (String) ((Map)record.get("instance")).get("hrid") );
            }
            writtenIDs.removeAll(failedHridsInBatch);
            if (failedHridsInBatch.isEmpty()) {
                Storage.log("Wrote " + writtenIDs.size() + " records to FOLIO: " + writtenIDs);
            } else {

                // Mark failed exports for human scrutiny
                for (int j = 0; j < failedHridsInBatch.size(); ++j) {
                    String failedHrid = failedHridsInBatch.get(j);
                    String message = errorMessagesInBatch.get(j);
                    String sql = """
                            INSERT OR REPLACE INTO export_failures (hrid, short_message, time) VALUES(?, ?, ?);
                            """;
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, failedHrid);
                        statement.setString(2, message);
                        statement.setString(3, ZonedDateTime.now().toString());
                        statement.execute();
                    }
                }

                Storage.log("Wrote " + writtenIDs.size() + " records to FOLIO: " + writtenIDs + " The following should have been written but were rejected: " + failedHridsInBatch);
            }
            // Clear any previous failed exports that have now been resolved.
            for (String writtenHrid : writtenIDs) {
                String sql = """
                            DELETE FROM export_failures WHERE hrid = ?;
                            """;
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, writtenHrid);
                    statement.execute();
                }
            }
            batch.clear();
        } catch (IOException | URISyntaxException | ParseException e) {
            Storage.log("Unexpected. ", e);
        }

    }
}
