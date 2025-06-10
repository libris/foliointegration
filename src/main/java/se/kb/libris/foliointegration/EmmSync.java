package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EmmSync {
    public final static String SYNCED_UNTIL_KEY = "EMMSyncStateSyncedUntil";

    private static List<String> SIGEL_LIST = Arrays.asList( System.getenv("SIGEL").split(",") );

    public static void run() {
        Connection connection = Storage.getConnection();
        long syncedUntil = Long.parseLong( Storage.getState(SYNCED_UNTIL_KEY, connection) );

        long newUntilTarget = syncedUntil + 2 * 60 * 1000; // Take (up to) 2 minutes of changes per iteration.
        long now = new Date().toInstant().toEpochMilli();
        if (newUntilTarget > now)
            newUntilTarget = now;

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

    private static void handleEmmActivity(Map<String, ?> activity, Connection connection) throws IOException, SQLException {
        Map<String,?> activityObject = (Map<String,?>) activity.get("object");

        switch ( (String)activity.get("type") ) {
            case "Create": {
                if (activityObject.containsKey("kbv:heldBy")) {
                    String heldBy = (String) ((Map) activityObject.get("kbv:heldBy")).get("@id");
                    String libraryCode = heldBy.substring(heldBy.lastIndexOf('/') + 1);
                    if (SIGEL_LIST.contains(libraryCode)) {
                        String response = Records.downloadJsonLdWithRetry((String) activityObject.get("id"));
                        Map record = Storage.mapper.readValue(response, Map.class);
                        if (record.containsKey("@graph")) {
                            List<Map> graphList = (List<Map>) record.get("@graph");

                            List<String> dependenciesToDownload = Records.collectUrisReferencedByThisRecord(graphList.get(1));
                            Records.filterUrisWeAlreadyHave(dependenciesToDownload, connection);
                            List<Map> dependencies = Records.downloadDependencies(dependenciesToDownload);
                            Records.writeRecord(graphList.get(1), connection);
                            for (Map dependency : dependencies) {
                                Records.writeRecord(dependency, connection);
                            }
                            System.err.println("Create of " + libraryCode + " -> " + activityObject.get("id"));
                        }
                    }
                }
                break;
            }
            case "Update":
                try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM uris WHERE URI = ?")) {
                    statement.setString(1, (String) activityObject.get("id"));
                    statement.execute();
                    try (ResultSet resultSet = statement.getResultSet()) {
                        if (resultSet.next()) {
                            // An update of an ID we *have*, that's all we need to know.
                            String response = Records.downloadJsonLdWithRetry( (String) activityObject.get("id") );
                            Map dependency = Storage.mapper.readValue(response, Map.class);
                            if (dependency.containsKey("@graph")) {
                                List<Map> graphList = (List<Map>) dependency.get("@graph");
                                Records.writeRecord(graphList.get(1), connection);
                                System.err.println("Update of -> " + activityObject.get("id"));
                            }
                        }
                    }
                }

                break;
            case "Delete": {
                try (PreparedStatement statement = connection.prepareStatement("DELETE FROM uris WHERE URI = ?")) {
                    statement.setString(1, (String) activityObject.get("id"));
                    statement.execute();
                }
                System.err.println("Delete of -> " + activityObject.get("id"));
                break;
            }
        }
        //Records.writeRecord();
    }
}
