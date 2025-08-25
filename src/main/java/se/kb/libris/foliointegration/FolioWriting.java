package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FolioWriting {
    private static final String username;
    private static final String password;
    private static final String folioBaseUri;
    private static final String folioTenant;

    private static List<Map> batch = new ArrayList<>();
    private static List<Thread> hridLookupThreads = new ArrayList<>();

    private static long folioTokenValidUntil = 0;
    private static String folioToken = null;

    static {
        username = System.getenv("FOLIOUSER");
        password = System.getenv("FOLIOPASS");
        folioBaseUri = System.getenv("OKAPI_URL");
        folioTenant = System.getenv("OKAPI_TENANT");
    }

    private static String getToken() {

        // This will seem weird, but we want key handling to be synchronized,
        // but SEPARATELY from the below queue/flush calls (which synchronize over the class)
        synchronized (password) {
            if (System.currentTimeMillis() < folioTokenValidUntil)
                return folioToken;

            for (int i = 0; i < 10; ++i) {
                try (HttpClient client = HttpClient.newHttpClient()) {
                    URI uri = new URI(folioBaseUri);
                    uri = uri.resolve("/authn/login-with-expiry");
                    var requestBodyMap = Map.of("tenant", folioTenant, "username", username, "password", password);
                    String requestBody = Storage.mapper.writeValueAsString(requestBodyMap);

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(uri)
                            .header("X-Okapi-Tenant", folioTenant)
                            .header("Accept", "application/json")
                            .header("Content-type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    List<String> tokens = response.headers().allValues("Set-Cookie");
                    for (String token : tokens) {
                        if (token.startsWith("folioAccessToken")) {

                            // We will need the token max age.
                            String[] parts = token.split(";"); // Separate cookie parts
                            for (int j = 0; j < parts.length; ++j) {
                                if (parts[j].trim().startsWith("Max-Age=")) {
                                    String maxAgeSeconds = parts[j].substring(9);
                                    folioTokenValidUntil = Long.parseLong(maxAgeSeconds) * 1000 + System.currentTimeMillis() - 60000; // Keep a 1 minute margin
                                }
                            }

                            folioToken = token;
                            return folioToken;
                        }
                    }
                    Storage.log("Unexpected FOLIO login response: " + response.body() + " / " + response.headers());
                } catch (IOException | URISyntaxException | InterruptedException e) {
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

    private static void lookupFolioHRID(Map folioRecord) {

        String token = getToken();

        Map instanceToBeSent = (Map) folioRecord.get("instance");
        String mainEntityUri = (String) instanceToBeSent.get("sourceUri");

        while (true) {
            try (HttpClient client = HttpClient.newHttpClient()) {
                URI uri = new URI(folioBaseUri);
                uri = uri.resolve("/inventory/instances?query=sourceUri==" + URLEncoder.encode("\"" + mainEntityUri + "\"", StandardCharsets.UTF_8));

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header("X-Okapi-Tenant", folioTenant)
                        .header("Accept", "application/json")
                        // .header("x-okapi-token", token) // old style login
                        .header("Cookie", token) // new style "rdr"-login
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    Storage.log("Failed FOLIO lookup: " + response + " / " + response.body());
                    return;
                }

                Map responseMap = Storage.mapper.readValue(response.body(), Map.class);
                if (responseMap.containsKey("instances")) {
                    List instances = (List) responseMap.get("instances");
                    if (!instances.isEmpty()) {
                        Map instanceFromFolio = (Map) instances.get(0); // There should never be more than one instance having this specifik ID
                        if (instanceFromFolio.containsKey("hrid")) {
                            //Storage.log("Replaced outgoing HRID: " + instanceFromFolio.get("hrid"));
                            instanceToBeSent.put("hrid", instanceFromFolio.get("hrid"));
                            return;
                        }
                    } else {
                        // NO HRID OBATINED FROM FOLIO, NEED A NEW ONE!
                        return;
                    }
                }
            } catch (IOException | URISyntaxException | InterruptedException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    // ignore
                }
                Storage.log("Retrying HRID lookup for: " + mainEntityUri);
            }
        }
    }

    public static synchronized void queueForExport(Map _folioRecord) throws IOException, InterruptedException {
        HashMap folioRecord = new HashMap(_folioRecord);
        batch.add(folioRecord);

        Thread t = Thread.startVirtualThread(() -> lookupFolioHRID(folioRecord));
        hridLookupThreads.add(t);

        if (batch.size() > 20) { // Too large batches results in internal http 414 in folio.
            flushQueue();
        }
    }

    public static synchronized void flushQueue() throws IOException, InterruptedException {

        // All HRID lookups must have concluded before flushing is possible
        for (Thread t : hridLookupThreads) {
            t.join();
        }
        hridLookupThreads.clear();

        if (batch.isEmpty())
            return;

        String token = getToken();

        Map recordSet = Map.of("inventoryRecordSets", batch);
        String body = Storage.mapper.writeValueAsString(recordSet);

        for (int i = 0; i < 10; ++i) {
            try (HttpClient client = HttpClient.newHttpClient()) {
                URI uri = new URI(folioBaseUri);
                uri = uri.resolve("/inventory-batch-upsert-hrid");

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header("X-Okapi-Tenant", folioTenant)
                        .header("Accept", "application/json")
                        .header("Content-type", "application/json")
                        // .header("x-okapi-token", token) // old style login
                        .header("Cookie", token) // new style "rdr"-login
                        .PUT(HttpRequest.BodyPublishers.ofString(body))
                        .build();

                //System.err.println("SENDING UPSERT: " + uri.toString());
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    Storage.log("Failed FOLIO write: " + response + " / " + response.body());
                    continue;
                }
                // IF OK
                Storage.log("Synced " + batch.size() + " records to FOLIO.");
                batch.clear();
                return;
            } catch (IOException | URISyntaxException | InterruptedException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
        }

        // All retries failed.
        throw new IOException("Writing to FOLIO failed, even with retries.");
    }
}
