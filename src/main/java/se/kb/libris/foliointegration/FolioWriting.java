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

    public static String lookupFolioHRID(String mainEntityUri) {
        /*
         curl --location --request GET 'https://okapi-folio-snapshot.okd-test.kb.se/inventory/instances?query=sourceUri==%22https://libris.kb.se/j2vwxtkv2rnjnhx%23it"' \
            --header 'x-okapi-tenant: kbtest1' \
            --header 'Content-Type: application/json' \
            --header 'Accept: application/json' \
            --header 'x-okapi-token: <TOKEN>'
         */

        /*

        {
  "instances" : [ {
    "id" : "85ced925-ad7a-4762-bad6-93ba162481e5",
    "_version" : "4",
    "hrid" : "http://libris.kb.se.localhost:5000/tc5blhs5383s47b#it",
    "source" : "LIBRIS",
    "title" : "Allting har sitt pris",
    "administrativeNotes" : [ ],
    "sourceUri" : "http://libris.kb.se.localhost:5000/tc5blhs5383s47b#it",
    "parentInstances" : [ ],
    "childInstances" : [ ],
    "isBoundWith" : false,
    "alternativeTitles" : [ ],
    "editions" : [ ],
    "series" : [ ],
    "identifiers" : [ ],
    "contributors" : [ ],
    "subjects" : [ ],
    "classifications" : [ ],
    "publication" : [ ],
    "publicationFrequency" : [ ],
    "publicationRange" : [ ],
    "electronicAccess" : [ ],
    "instanceTypeId" : "30fffe0e-e985-4144-b2e2-1e8179bdb41f",
    "instanceFormatIds" : [ ],
    "physicalDescriptions" : [ ],
    "languages" : [ ],
    "notes" : [ ],
    "previouslyHeld" : false,
    "staffSuppress" : false,
    "discoverySuppress" : false,
    "deleted" : false,
    "statisticalCodeIds" : [ ],
    "statusUpdatedDate" : "2025-08-21T08:33:30.634+0000",
    "metadata" : {
      "createdDate" : "2025-08-21T07:29:25.178+00:00",
      "createdByUserId" : "661487b1-5ad6-4982-bd2a-7ac2462e52f3",
      "updatedDate" : "2025-08-21T08:33:30.635+00:00",
      "updatedByUserId" : "661487b1-5ad6-4982-bd2a-7ac2462e52f3"
    },
    "tags" : {
      "tagList" : [ ]
    },
    "natureOfContentTermIds" : [ ],
    "precedingTitles" : [ ],
    "succeedingTitles" : [ ]
  } ],
  "totalRecords" : 1
}


         */
        String token = getToken(); // TEMP! REUSE! DO NOT GET A NEW ONE FOR EVERY REQUEST.

        try (HttpClient client = HttpClient.newHttpClient()) {
            URI uri = new URI(folioBaseUri);
            uri = uri.resolve("/inventory/instances?query=sourceUri==" + URLEncoder.encode("\"" + mainEntityUri + "\"", StandardCharsets.UTF_8));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .header("X-Okapi-Tenant", folioTenant)
                    .header("Accept", "application/json")
                    .header("x-okapi-token", token)
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                Storage.log("Failed FOLIO lookup: " + response + " / " + response.body());
                return null;
            }

            //Storage.log("source lookup: " + response.body());

            Map responseMap = Storage.mapper.readValue(response.body(), Map.class);
            if (responseMap.containsKey("instances")) {
                List instances = (List) responseMap.get("instances");
                if (!instances.isEmpty()) {
                    Map instance = (Map) instances.get(0); // There should never be more than one instance having this specifik ID
                    if (instance.containsKey("hrid")) {
                        return (String) instance.get("hrid");
                    }
                }
            }
        } catch (IOException | URISyntaxException | InterruptedException e) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e2) {
                // ignore
            }
        }

        return null;
    }

    public static synchronized void queueForExport(Map mainEntity) throws IOException {
        batch.add(mainEntity);

        if (batch.size() > 10) { // Too large batches results in internal http 414 in folio.
            flushQueue();
        }
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
