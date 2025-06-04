package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

public class EmmSync {
    public final static String SYNCED_UNTIL_KEY = "SyncStateSyncedUntil";

    public static void run() {
        Connection connection = Storage.getConnection();
        long syncedUntil = Long.parseLong( Storage.getState(SYNCED_UNTIL_KEY, connection) );

        long newUntilTarget = syncedUntil + 2 * 60 * 1000; // Take (up to) 2 minutes of changes per iteration.

        try (HttpClient client = HttpClient.newHttpClient()) {
            URI uri = new URI(System.getenv("EMMBASEURL")).resolve("?until=" + newUntilTarget);
            while (uri != null) {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                Map responseMap = Storage.mapper.readValue(response.body(), Map.class);

                List<?> items = (List<?>) responseMap.get("orderedItems");
                // DO UPDATES HERE

                Map<String, ?> last = (Map<String, ?>) items.getLast();
                long earliestTimeOnPage = ZonedDateTime.parse((String) last.get("published")).toInstant().toEpochMilli();
                if (earliestTimeOnPage < syncedUntil) {
                    Storage.writeState(SYNCED_UNTIL_KEY, "" + newUntilTarget, connection);
                    connection.commit(); // Time stamp and updated data together
                    Storage.log("Now synced up until: " + newUntilTarget + " (" + Instant.ofEpochMilli(newUntilTarget) + ")");
                    uri = null;
                } else {
                    uri = new URI( (String)responseMap.get("next") );
                }
            }
        } catch (URISyntaxException | IOException | InterruptedException | SQLException e) {
            Storage.log("Sync iteration failed.", e);
        }

    }
}
