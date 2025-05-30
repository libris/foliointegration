package se.kb.libris.foliointegration;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;


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
        {
            String offset = Storage.getState(OFFSET_KEY);
            if (offset == null) {
                Storage.writeState(OFFSET_KEY, "" + 0);
            }
        }


        Deque<Thread> pageQueue = new ArrayDeque<>();

        // Download a page
        String offset = Storage.getState(OFFSET_KEY);
    }

    private static void downloadPage(String offset, Deque<Thread> pageQueue) {
        try (HttpClient client = HttpClient.newHttpClient()) {
            URI uri = new URI(emmBaseUrl).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<?, ?> responseMap = mapper.readValue(response.body(), Map.class);

            // Possibly pre-fetch more pages
            /*
            String NEXTOFFSET = "100"; // TEMP
            synchronized (pageQueue) {
                if (pageQueue.size() < maxThreads) {
                    Thread.ofPlatform().name("Page fetch").start(new Runnable() {
                        public void run() {
                            downloadPage(NEXTOFFSET, pageQueue);
                        }
                    });
                }
            }*/

        } catch (URISyntaxException | IOException | InterruptedException e) {
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
