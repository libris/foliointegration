package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FolioWriting {
    private static final String username;
    private static final String password;
    private static final String folioBaseUri;
    private static final String folioTenant;

    private static List<Map> batch = new ArrayList<>();

    static {
        username = System.getenv("FOLIOUSER");
        password = System.getenv("FOLIOPASS");
        folioBaseUri = System.getenv("OKAPI_URL");
        folioTenant = System.getenv("OKAPI_TENANT");
    }

    private static String getToken() {
        for (int i = 0; i < 10; ++i) {
            try (HttpClient client = HttpClient.newHttpClient()) {
                URI uri = new URI(folioBaseUri);
                uri = uri.resolve("/authn/login");
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
                Map responseMap;
                try {
                    responseMap = Storage.mapper.readValue(response.body(), Map.class);
                } catch (org.codehaus.jackson.JsonParseException pe) {
                    Storage.log("Unexpected token response: " + response.body());
                    continue;
                }
                if (responseMap.containsKey("okapiToken")) {
                    return (String) responseMap.get("okapiToken");
                } else {
                    Storage.log("Unexpected FOLIO login response: " + responseMap);
                }
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

    public static synchronized void queueForExport(Map mainEntity) {
        batch.add(mainEntity);
    }

    public static synchronized void flushQueue() throws IOException {
        if (batch.isEmpty())
            return;

        String token = getToken(); // TEMP! REUSE! DO NOT GET A NEW ONE FOR EVERY REQUEST.

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
                        .header("x-okapi-token", token)
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
