package se.kb.libris.foliointegration;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.*;


public class EmmDumpImporter {

    private final static String OFFSET_KEY = "DumpStateOffset";
    private final static String DUMP_ID_KEY = "DumpStateCreationTime";

    public static final ObjectMapper mapper = new ObjectMapper();

    static String emmBaseUrl = "https://libris-qa.kb.se/api/emm/"; // temp
    static String sigel = "X"; // temp
    static int maxThreads = 4; // temp

    public static void run() {

        startIfNotStarted();

        // Set initial offset if needed
        String offset = Storage.getState(OFFSET_KEY);
        if (offset == null) {
            Storage.writeState(OFFSET_KEY, "" + 1); // 1 to skip initial context bs.
        }

        offset = Storage.getState(OFFSET_KEY);
        String dumpId = Storage.getState(DUMP_ID_KEY);

        try (HttpClient client = HttpClient.newHttpClient()) {
            URI uri = new URI(emmBaseUrl).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);
            while (uri != null) {
                System.err.println(uri);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                Map<?, ?> responseMap = mapper.readValue(response.body(), Map.class);

                if (responseMap.containsKey("next")) {
                    uri = new URI( (String) responseMap.get("next") );
                } else {
                    uri = null;
                }

                if (responseMap.containsKey("startTime")) {
                    if (!dumpId.equals( responseMap.get("startTime") )) {
                        return; // This will result in a restart (now that the timestamp has changed)
                    }
                }

                if (responseMap.containsKey("items")) {
                    List<?> items = (List<?>) responseMap.get("items");
                    offset = "" + (Integer.parseInt(offset) + items.size());

                    Connection connection = Storage.getConnection();
                    for (Object item : items) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> itemMap = (Map<String, Object>) item;
                        if (itemMap.containsKey("@graph")) {
                            HashMap<String, Object> docMap = new HashMap<>();
                            docMap.put("@graph", itemMap.get("@graph")); // We just want the graph list, not the other attached stuff
                            // Write records here..
                        }
                    }
                    String sql = "UPDATE state SET value = ? WHERE key = ?;";
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, offset);
                        statement.setString(2, OFFSET_KEY);
                        statement.execute();
                    }

                    // Do actual writes first..

                    connection.commit();
                }
            }
        } catch (URISyntaxException | IOException | InterruptedException | SQLException e) {
            Storage.log("Page download failed (will be retried).", e);
        }

    }

    private static void startIfNotStarted() {
        try (HttpClient client = HttpClient.newHttpClient()) {

            URI uri = new URI(emmBaseUrl).resolve("full?selection=itemAndInstance:" + sigel + "&offset=0");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<?, ?> responseMap = mapper.readValue(response.body(), Map.class);
            if (responseMap.containsKey("startTime")) {
                String startTime = (String) responseMap.get("startTime");
                ZonedDateTime.parse(startTime); // parses correctly or throws

                String dumpId = Storage.getState(DUMP_ID_KEY);
                if (dumpId == null) { // This means no dump download is in progress. Time to start.
                    Storage.log("Starting download of EMM dump with creation time: " + startTime);
                    Storage.clearState(OFFSET_KEY);
                    Storage.writeState(DUMP_ID_KEY, startTime);
                } else if (!dumpId.equals(startTime)) {
                    Storage.log("EMM dump is now stale, a restart is necessary. Will now download dump with creation time: " + startTime);
                    Storage.clearState(OFFSET_KEY);
                    Storage.writeState(DUMP_ID_KEY, startTime);
                }
            }

        } catch (URISyntaxException | IOException | InterruptedException e) {
            Storage.log("Failed to start dump download.", e);
        }
    }

}
