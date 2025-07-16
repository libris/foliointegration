package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

public class FolioWriting {
    private static final String username;
    private static final String password;
    private static final String folioBaseUri;
    private static final String folioTenant;

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
                Map responseMap = Storage.mapper.readValue(response.body(), Map.class);
                if (responseMap.containsKey("okapiToken")) {
                    return (String) responseMap.get("okapiToken");
                } else {
                    Storage.log("Unexpected FOLIO login response: " + responseMap);
                }
            } catch (IOException | URISyntaxException | InterruptedException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
        }

        return null;
    }

    private static void doMIUupsert(String token) {

        // WHAT TO SEND:
        // https://github.com/folio-org/mod-inventory-update/blob/master/ramls/inventory-record-set-with-hrids.json

        // DOCS:
        // https://github.com/folio-org/mod-inventory-update/blob/master/ramls/inventory-update.raml

        // Minimum required properties, "instance" object with [ "source", "title", "instanceTypeId", "hrid" ] as determined by the json-schema.

        String body = """
                {
                "instance":
                {
                    "hrid": "abcdef",
                    "title": "some title",
                    "source": "LIBRIS",
                    "instanceTypeId": "Print"
                },
                "holdingsRecords":[]
                }
                """;


        for (int i = 0; i < 10; ++i) {
            try (HttpClient client = HttpClient.newHttpClient()) {
                URI uri = new URI(folioBaseUri);
                uri = uri.resolve("/inventory-upsert-hrid");

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header("X-Okapi-Tenant", folioTenant)
                        .header("Accept", "application/json")
                        .header("Content-type", "application/json")
                        .header("x-okapi-token", token)
                        .PUT(HttpRequest.BodyPublishers.ofString(body))
                        .build();

                System.err.println("SENDING UPSERT: " + uri.toString());
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                System.err.println("UPSERT RESPONSE: " + response + " / " + response.body());
                // IF OK
                return;
            } catch (IOException | URISyntaxException | InterruptedException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
        }
    }

    public static void testWrite() {

        // THIS SHIT IS WORK-IN-PROGRESS. NOT YET USABLE.

        String token = getToken();
        System.err.println("FOLIO TOKEN:" + token);
        doMIUupsert(token);
    }
}
