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
import java.util.concurrent.ConcurrentHashMap;


public class EmmDumpImporter {

    private final static String OFFSET_KEY = "DumpStateOffset";
    private final static String DUMP_ID_KEY = "DumpStateCreationTime";

    public static final ObjectMapper mapper = new ObjectMapper();

    static String emmBaseUrl = "https://libris-qa.kb.se/api/emm/"; // temp
    static String sigel = "X"; // temp
    static int maxThreads = 8; // temp

    static final ConcurrentHashMap<String, Map<String, ?>> prefetchedPages = new ConcurrentHashMap<>();

    public static void run() {

        startIfNotStarted();

        // Set initial offset if needed
        String offset = Storage.getState(OFFSET_KEY);
        if (offset == null) {
            Storage.writeState(OFFSET_KEY, "" + 1); // 1 to skip initial context bs.
        }

        offset = Storage.getState(OFFSET_KEY);
        String dumpId = Storage.getState(DUMP_ID_KEY);

        try {
            URI uri = new URI(emmBaseUrl).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);
            while (uri != null) {
                Map<String, ?> responseMap = getPage(Integer.parseInt(offset));

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
        } catch (URISyntaxException | SQLException e) {
            Storage.log("Page download failed (will be retried).", e);
        }

    }

    /**
     * Call only from the main thread, or risk a race condition.
     */
    private static void prefetchPage(int offset) {
        try {
            URI uri = new URI(emmBaseUrl).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);

            // Place an empty map with this key when starting. This will be a signal that the page is already being
            // downloaded, and should be waited for rather than re-downloaded.
            if (prefetchedPages.containsKey(uri.toString()))
                return;
            prefetchedPages.put(uri.toString(), new HashMap<>());

            Thread.ofPlatform().name("EMM prefetch").start(new Runnable() {
                public void run() {
                    try (HttpClient client = HttpClient.newHttpClient()) {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(uri)
                            .GET()
                            .build();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    prefetchedPages.put( uri.toString(), mapper.readValue(response.body(), Map.class) );
                    } catch (IOException | InterruptedException e) {
                        prefetchedPages.remove(uri.toString());
                        Storage.log("Page prefetch failed.", e);
                    }
                }
            });

        } catch (URISyntaxException e) {
            Storage.log("Page prefetch failed.", e);
        }
    }

    private static Map<String, ?> getPage(int offset) {
        System.err.println("Fetching at offset: " + offset);

        // Start prefetching of subsequent pages, if not already on the way:
        for (int i = 0; i < maxThreads; ++i) {
            prefetchPage(offset + 100 * i); // That the dump page size is 100 is an assumption, which MUST hold true for this to work correctly.
        }

        try {
            URI uri = new URI(emmBaseUrl).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);

            var emptyMap = new HashMap<>();
            var page = prefetchedPages.get(uri.toString());
            while (page == null || page.equals(emptyMap)) {
                Thread.sleep(1);
                page = prefetchedPages.get(uri.toString());
            }
            prefetchedPages.remove(uri.toString());
            return page;

        } catch (URISyntaxException | InterruptedException e) {
            Storage.log("Page download failed (will be retried).", e);
        }
        return null;
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
