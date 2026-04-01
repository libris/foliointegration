package se.kb.libris.foliointegration;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
                try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
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

                    ClassicHttpResponse response = httpClient.execute(request);
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
            try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

                URI uri = new URI(folioBaseUri);
                uri = uri.resolve(pathAndParameters);
                HttpGet request = new HttpGet(uri);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);

                request.setHeader("X-Okapi-Tenant", folioTenant);
                request.setHeader("Accept", "application/json");
                request.setHeader("Cookie", token);

                ClassicHttpResponse response = httpClient.execute(request);
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

    public static synchronized void queueForExport(Map _folioRecord, Connection connection) throws IOException, InterruptedException, SQLException {
        // Make a copy, as we will be making some slight changes in there
        HashMap folioRecord = new HashMap(_folioRecord);

        // We want to send holding records, without items, but without deleting existing items.
        folioRecord.put("processing",
                        Map.of("item",
                                Map.of("retainOmittedRecord",
                                        Map.of("ifField", "hrid", "matchesPattern", ".*"))
                        ));

        batch.add(folioRecord);

        if (batch.size() >= folioWriteBatchSize) { // Too large batches results in internal http 414 in folio.
            flushQueue(connection);
        }
    }

    /*
    This is a huge complication, due to the fact that we have to "create items" in folio, but only once, after the
    creation of a holding record. This is very att odds with the rest of the system, which is built to *keep things
    up date*.

    The way this works is that when an EMM creation event (of one of our holdings) is detected, it gets put in table
    of its own, 'holding_creations'. Items are then "always created" in formatting, even on updates, because otherwise
    the checksum system wouldn't work. But only if a holding has its ID in the holding_creations-table will we actually
    write those items to folio. This function clears out the items for any of the batched records it is passed that DOES
    NOT have the correct holding ID in 'holding_creations', so that we do not create new items. It also returns
    something like the following:
        {
          "instanceHRID1": ["holdingHRID1", "holdingHRID1"]
        }
    Meaning: If instanceHRID1 is written successfully, consider holdingHRID1 and holdingHRID2 to have created their
    items (which CANNOT EVER BE ALLOWED to happen more than once).

    This is because instances, holdings and items are written together in one go. And the instance ID is the one we
    will know succeeded or failed. So after a successful write, we check instanceIDs written against the list returned
    from here, and then clear all of the associated holdingIDs from 'holding_creations', so that we only create these
    items exactly once.
     */
    private static HashMap<String, ArrayList<String>> clearItemsUnlessAllowed(List<Map> recordsToBeWritten, Connection connection) throws SQLException {
        var instanceHRIDsToHoldingsHRIDsWithItems = new HashMap<String, ArrayList<String>>();
        for (Map recordToBeWritten : recordsToBeWritten) {

            ArrayList<String> holdingHRIDsWithItems = new ArrayList<>();
            String instanceHRID = (String) ((Map)recordToBeWritten.get("instance")).get("hrid");
            instanceHRIDsToHoldingsHRIDsWithItems.put(instanceHRID, holdingHRIDsWithItems);

            if (recordToBeWritten.containsKey("holdingsRecords")) {
                for (Map folioHolding : (List<Map>) recordToBeWritten.get("holdingsRecords")) {

                    boolean shouldCreateItems = false;
                    try (PreparedStatement statement = connection.prepareStatement("SELECT hrid FROM holding_creations WHERE hrid = ?")) {
                        statement.setString(1, (String) folioHolding.get("hrid"));
                        statement.execute();
                        try (ResultSet resultSet = statement.getResultSet()) {
                            if (resultSet.next()) {
                                shouldCreateItems = true;
                            }
                        }
                    }
                    if (!shouldCreateItems) {
                        folioHolding.remove("items");
                        //Storage.log("Filtering items of " + folioHolding.get("hrid") + " because no found creation-event.");
                    } else {
                        Storage.log("Allowing the creation of " + ( (List) folioHolding.get("items") ).size() + " items for " + folioHolding.get("hrid"));
                        String holdingHRID = (String) folioHolding.get("hrid");
                        holdingHRIDsWithItems.add(holdingHRID);
                    }
                }
            }
        }
        return instanceHRIDsToHoldingsHRIDsWithItems;
    }

    private static String getNextBarCode(CloseableHttpClient httpClient, String token) throws IOException, URISyntaxException, ParseException {

        //  https://okapi-folio-snapshot.okd-kv.kb.se/servint/numberGenerators/getNextNumber?generator=inventory_itemBarcode&sequence=itemBarcode

        URI uri = new URI(folioBaseUri);
        uri = uri.resolve("/servint/numberGenerators/getNextNumber?generator=inventory_itemBarcode&sequence=itemBarcode");
        HttpGet request = new HttpGet(uri);
        RequestConfig config = RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
        request.setConfig(config);

        request.setHeader("X-Okapi-Tenant", folioTenant);
        request.setHeader("Accept", "application/json");
        request.setHeader("Cookie", token);

        ClassicHttpResponse response = httpClient.execute(request);
        String responseText = EntityUtils.toString(response.getEntity());

        // respone looks like so: {"generator":"inventory_itemBarcode","sequence":"itemBarcode","status":"OK","nextValue":"0000000003"}
        Map responseMap = Storage.mapper.readValue(responseText, Map.class);
        if (responseMap.get("status").equals("OK") && responseMap.get("nextValue") != null && responseMap.get("nextValue") instanceof String code) {
            return code;
        }
        throw new RuntimeException("Failure getting next barcode from folio. Response was: " + responseText);
    }

    private static void insertBarCodes(CloseableHttpClient httpClient, String token, Object node) throws IOException, URISyntaxException, ParseException {
        if (node instanceof Map m) {

            Object keyToReplace = null;
            for(Object key : m.keySet()) {
                if (m.get(key).equals("__FOLIO_FETCH_BARCODE"))
                    keyToReplace = key;
            }
            if (keyToReplace != null) {
                m.put(keyToReplace, getNextBarCode(httpClient, token));
                return;
            }

            // Move along
            for(Object key : m.keySet()) {
                if (m.get(key) != null)
                    insertBarCodes(httpClient, token, m.get(key));
            }
        } else if (node instanceof List l) {
            for (int i = 0; i < l.size(); ++i) {
                if (l.get(i) instanceof String s && s.equals("__FOLIO_FETCH_BARCODE")) {
                    l.remove(i);
                    l.add(i, getNextBarCode(httpClient, token));
                }
            }

            // Move along
            for (Object element : l) {
                if (element != null)
                    insertBarCodes(httpClient, token, element);
            }
        }
    }

    public static synchronized void flushQueue(Connection connection) throws IOException, InterruptedException, SQLException {
        if (batch.isEmpty())
            return;

        HashMap<String, ArrayList<String>> instanceHRIDsToHoldingsHRIDsWithItems = clearItemsUnlessAllowed(batch, connection);

        //Storage.log("BATCH FIRST:  " + Storage.mapper.writeValueAsString( batch.getFirst() ) );
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

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
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

            // Fetch barcodes from FOLIO and insert them now (at the last possible instant).
            insertBarCodes(httpClient, token, batch);

            Map recordSet = Map.of("inventoryRecordSets", batch);
            String body = Storage.mapper.writeValueAsString(recordSet);

            //Storage.log(" SENDING: " + body);

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

            ClassicHttpResponse response = httpClient.execute(request);
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
                                //Storage.log("FOLIO rejection: " + Storage.mapper.writeValueAsString(error) );
                                Storage.log("FOLIO rejection: " + Storage.mapper.writeValueAsString(error.get("message")) );

                                if ( error.get("entity") instanceof Map requesEntity) {
                                    if ( requesEntity.get("hrid") instanceof String hridBroken) {
                                        failedHridsInBatch.add(hridBroken);
                                    }
                                }
                                if ( error.get("message") != null) {
                                    errorMessagesInBatch.add(Storage.mapper.writeValueAsString(error.get("message")));
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
                    try (PreparedStatement statement = connection.prepareStatement("DELETE FROM exported_checksum WHERE hrid = ?")) {
                        statement.setString(1, failedHrid);
                        statement.execute();
                    }
                }

                Storage.log("Wrote " + writtenIDs.size() + " records to FOLIO: " + writtenIDs + " The following should have been written but were rejected: " + failedHridsInBatch);
            }

            for (String writtenHrid : writtenIDs) {
                // Clear any previous failed exports that have now been resolved.
                String sql = """
                            DELETE FROM export_failures WHERE hrid = ?;
                            """;
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, writtenHrid);
                    statement.execute();
                }

                // Clear any entries in the holding_creations table, if we WROTE an instance containing ITEMS (which we cannot ever do more than once per created holding)
                if (instanceHRIDsToHoldingsHRIDsWithItems.containsKey(writtenHrid)) {
                    List<String> holdingHRIDs = instanceHRIDsToHoldingsHRIDsWithItems.get(writtenHrid);
                    for (String holdingHRIDwithItemsWritten : holdingHRIDs) {

                        // We know the ID of the instant we wrote, from this and the instanceHRIDsToHoldingsHRIDsWithItems we derive the holding HRIDs we wrote
                        // and for each of those, consider it a success, only if there isnt also a listed failed HRID for an item associated with that holding.
                        boolean writtenOk = true;
                        for (String failedHrid : failedHridsInBatch) {
                            if (failedHrid.startsWith(holdingHRIDwithItemsWritten))
                                writtenOk = false;
                        }

                        if (writtenOk) {
                            sql = """
                                    DELETE FROM holding_creations WHERE hrid = ?;
                                    """;
                            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                                statement.setString(1, holdingHRIDwithItemsWritten);
                                statement.execute();
                            }
                            Storage.log("Cleared holding hrid: " + holdingHRIDwithItemsWritten + " from the item creation queue. Items for this holding have now been created and should never be created again.");
                        }
                    }
                }
            }
            batch.clear();
        } catch (IOException | URISyntaxException | ParseException e) {
            Storage.log("Unexpected. ", e);
        }

    }
}
