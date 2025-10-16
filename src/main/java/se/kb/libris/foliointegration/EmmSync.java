package se.kb.libris.foliointegration;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.*;

public class EmmSync {
    public final static String SYNCED_UNTIL_KEY = "EMMSyncStateSyncedUntil";
    public final static String LAST_TAKEN_CHANGE_KEY = "EMMSyncStateLastTakenChange";

    private static List<String> SIGEL_LIST = Arrays.asList( System.getenv("SIGEL").split(",") );


    private static final long timeTruncationErrorMargin = 100; // milliseconds

    public static boolean run() {
        Connection connection = Storage.getConnection();
        long syncedUntil = Long.parseLong( Storage.getState(SYNCED_UNTIL_KEY, connection) );
        String lastTakenChangeId = Storage.getState(LAST_TAKEN_CHANGE_KEY, connection);
        String newLastTakenChangeId = null;
        boolean changesMade = false;

        long newUntilTarget = syncedUntil + 2 * 60 * 1000; // Take (up to) 2 minutes of changes per iteration.
        long now = new Date().toInstant().toEpochMilli();
        if (newUntilTarget > now)
            newUntilTarget = now;

        URI uri = null;
        try {
            uri = new URI(System.getenv("EMM_BASE_URL")).resolve("?until=" + newUntilTarget);
            boolean foundAlreadyTakenChange = false;
            while (uri != null) {

                HttpGet request = new HttpGet(uri);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);
                request.setHeader("accept", "application/json+ld");
                ClassicHttpResponse response = Server.httpClient.execute(request);
                String responseString = EntityUtils.toString(response.getEntity());
                Map responseMap = Storage.mapper.readValue(responseString, Map.class);

                List<Map<String,?>> items = ((List<Map<String,?>>) responseMap.get("orderedItems"));
                for (Map<String, ?> item : items) {
                    long modified = ZonedDateTime.parse((String) item.get("published")).toInstant().toEpochMilli();
                    if (!foundAlreadyTakenChange) {
                        if (modified + timeTruncationErrorMargin > syncedUntil) {

                            String changeId = "" + item.get("published") + item.get("id");
                            if (newLastTakenChangeId == null)
                                newLastTakenChangeId = changeId;

                            if (!changeId.equals(lastTakenChangeId)) {
                                changesMade |= handleEmmActivity(item, connection);
                            } else {
                                foundAlreadyTakenChange = true;
                            }
                        }
                    }
                }

                Map<String, ?> latest = items.getLast();
                long earliestTimeOnPage = ZonedDateTime.parse((String) latest.get("published")).toInstant().toEpochMilli();
                if (earliestTimeOnPage < syncedUntil) {
                    Storage.writeState(SYNCED_UNTIL_KEY, "" + newUntilTarget, connection);
                    Storage.writeState(LAST_TAKEN_CHANGE_KEY, newLastTakenChangeId, connection);
                    connection.commit(); // Time stamp and updated data together
                    //Storage.log("Now synced up until: " + newUntilTarget + " (" + Instant.ofEpochMilli(newUntilTarget) + ")");
                    uri = null;
                } else {
                    uri = new URI( (String)responseMap.get("next") );
                }
            }

        } catch (URISyntaxException | IOException | SQLException | ParseException e) {
            Storage.log("Sync iteration request failed. (" + uri.toString() + ")", e);
            try {
                connection.rollback();
            } catch (SQLException se) {
                Storage.log("Iteration update rollback failed. Fatal.", se);
                System.exit(1);
            }
        }

        return changesMade;
    }

    private static boolean handleEmmActivity(Map<String, ?> activity, Connection connection) throws IOException, SQLException {
        Map<String,?> activityObject = (Map<String,?>) activity.get("object");

        boolean changesMade = false;
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

                            Set<String> dependenciesToDownload = Records.collectUrisReferencedByThisRecord(graphList.get(1));
                            Records.filterUrisWeAlreadyHave(dependenciesToDownload, connection);
                            List<Map> dependencies = Records.downloadDependencies(dependenciesToDownload);
                            Records.writeRecord(graphList.get(1), connection);

                            for (Map dependency : dependencies) {
                                Records.writeRecord(dependency, connection);
                            }
                            System.err.println("Create of " + libraryCode + " -> " + activityObject.get("id"));
                            changesMade = true;
                        }
                    }
                }
                break;
            }
            case "Update":
                try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM entities WHERE URI = ?")) {
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
                                changesMade = true;
                            }
                        }
                    }
                }

                break;
            case "Delete": {
                try (PreparedStatement statement = connection.prepareStatement("DELETE FROM entities WHERE URI = ?")) {
                    statement.setString(1, (String) activityObject.get("id"));
                    statement.execute();
                }
                System.err.println("Delete of -> " + activityObject.get("id"));
                break;
            }
        }

        return changesMade;
    }
}
