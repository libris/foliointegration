package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
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

                List<Map<String,?>> items = (List<Map<String,?>>) responseMap.get("orderedItems");
                for (Map<String, ?> item : items) {
                    long modified = ZonedDateTime.parse((String) item.get("published")).toInstant().toEpochMilli();
                    if (modified > syncedUntil) {
                        handleEmmActivity(item, connection);
                    }
                }

                Map<String, ?> last = items.getLast();
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
            try {
                connection.rollback();
            } catch (SQLException se) {
                Storage.log("Iteration update rollback failed. Fatal.", se);
                System.exit(1);
            }
        }

    }

    private static void handleEmmActivity(Map<String, ?> activity, Connection connection) throws SQLException {
        Map<String,?> activityObject = (Map<String,?>) activity.get("object");

        switch ( (String)activity.get("type") ) {
            case "Create": {
                break;
            }
            case "Update":
                break;
            case "Delete": {
                try (PreparedStatement statement = connection.prepareStatement("DELETE FROM uris WHERE URI = ?")) {
                    statement.setString(1, (String) activityObject.get("id"));
                    statement.execute();
                }
                break;
            }
        }
        //Records.writeRecord();
    }
}
