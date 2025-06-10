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
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class EmmDumpImport {

    private final static String OFFSET_KEY = "EMMDumpStateOffset";
    private final static String DUMP_ID_KEY = "EMMDumpStateCreationTime";
    private final static String DUMP_SIGEL_KEY = "EMMDumpStateSigel";
    private final static int maxThreads = 8;

    final static ConcurrentHashMap<String, Map<String, ?>> prefetchedPages = new ConcurrentHashMap<>();
    static String sigel;

    public static void run() throws Exception{
        Connection connection = Storage.getConnection();
        sigel = Storage.getState(DUMP_SIGEL_KEY, connection);
        if (sigel == null) {
            sigel = System.getenv("SIGEL").split(",")[0];
            Storage.writeState(DUMP_SIGEL_KEY, sigel, connection);
            connection.commit();
        }

        startIfNotStarted();

        String offset;
        String dumpId;
        // Set initial offset if needed
        offset = Storage.getState(OFFSET_KEY, connection);
        if (offset == null) {
            Storage.writeState(OFFSET_KEY, "" + 1, connection); // 1 to skip initial context bs.
        }

        offset = Storage.getState(OFFSET_KEY, connection);
        dumpId = Storage.getState(DUMP_ID_KEY, connection);
        connection.commit();

        try {
            URI uri = new URI(System.getenv("EMMBASEURL")).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);
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

                    // Do dependency downloads (not writes!) concurrently
                    var dependencies = Collections.synchronizedList(new ArrayList<Map>());
                    var threads = new ArrayList<Thread>(100);
                    for (Object item : items) {
                        Map<String, Object> itemMap = (Map<String, Object>) item;
                        List<?> graphList = (List<?>) itemMap.get("@graph");

                        List<String> dependenciesToDownload = Records.collectUrisReferencedByThisRecord(graphList.get(1));
                        Records.filterUrisWeAlreadyHave(dependenciesToDownload, connection);
                        Thread t = Thread.startVirtualThread(() -> dependencies.addAll(Records.downloadDependencies(dependenciesToDownload)));
                        threads.add(t);
                    }
                    for (Thread t : threads) {
                        t.join();
                    }

                    // Write record and dependencies
                    for (Object item : items) {
                        Map<String, Object> itemMap = (Map<String, Object>) item;
                        if (itemMap.containsKey("@graph")) {
                            List<Map<String,?>> graphList = (List<Map<String,?>>) itemMap.get("@graph");

                            // We take dumps in the "itemAndInstance" category, which means we get holding records
                            // with embedded instances. We don't want them embedded, we want them separate. So separate
                            // them into two distinct records again before writing (one Item-record and one Instance-record).
                            Map mainEntity = graphList.get(1);
                            Map itemOf = (Map) mainEntity.get("itemOf");
                            String instanceUri = (String) itemOf.get("@id");
                            mainEntity.put("itemOf", instanceUri);

                            //Map<String, ?> mainEntity = (Map<String, ?>) graphList.get(1);

                            Records.writeRecord(mainEntity, connection);
                            Records.writeRecord(itemOf, connection);
                        }
                    }
                    for (Map dependency : dependencies) {
                        Records.writeRecord(dependency, connection);
                    }

                    Storage.writeState(OFFSET_KEY, offset, connection);

                    connection.commit(); // The record writes AND our new consumed offset together

                }
            }

            // If/when we get here, the 'next' uri is null, meaning the dump download is finished.
            finalizeDumpDownload();

        } catch (URISyntaxException | SQLException e) {
            Storage.log("Page download failed (will be retried).", e);
        }

    }

    private static void finalizeDumpDownload() {
        Storage.log("EMM dump download complete, for sigel: " + sigel);

        try {
            Connection connection = Storage.getConnection();

            // Set initial time for sync catch-up
            ZonedDateTime dumpCreationTime = ZonedDateTime.parse( Storage.getState(DUMP_ID_KEY, connection) );
            long candidateUntil = dumpCreationTime.toInstant().toEpochMilli();
            long preExistingUntil = 33305941930000L; // assumption, *far* future. (Instant.MAX overflows unfortunately)
            String preExistingUntilString = Storage.getState(EmmSync.SYNCED_UNTIL_KEY, connection);
            if (preExistingUntilString != null) {
                preExistingUntil = Long.parseLong(preExistingUntilString);
            }
            if (candidateUntil < preExistingUntil) {
                Storage.writeState(EmmSync.SYNCED_UNTIL_KEY, ""+candidateUntil, connection);
                Storage.log("Sync-from time is now set to: " + candidateUntil + " (" + dumpCreationTime + ")");
            }

            Storage.clearState(OFFSET_KEY, connection);
            Storage.clearState(DUMP_ID_KEY, connection);

            // If there are more dumps to download, set starting conditions for the next one.
            String[] sigelList = System.getenv("SIGEL").split(",");
            int currentSigelIndex = 0;
            for (int i = 0; i < sigelList.length; ++i) {
                if (sigel.equals(sigelList[i])) {
                    currentSigelIndex = i;
                    break;
                }
            }
            if (currentSigelIndex < sigelList.length - 1) {
                sigel = sigelList[currentSigelIndex + 1];

                Storage.writeState(DUMP_SIGEL_KEY, sigel, connection);
            } else {
                Storage.clearState(DUMP_SIGEL_KEY, connection);
                Storage.writeState(FolioSync.SYNCED_UNTIL_KEY, "0", connection);
                Storage.transitionToApplicationState(Storage.APPLICATION_STATE.INITIAL_LOAD_TO_FOLIO, connection);
            }

            connection.commit();
        } catch (SQLException e) {
            Storage.log("Dump finalization failed. Fatal.", e);
            System.exit(1);
        }
    }

    /**
     * Call only from the main thread, or risk a race condition.
     */
    private static void prefetchPage(int offset) {
        try {
            URI uri = new URI(System.getenv("EMMBASEURL")).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);

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
                    prefetchedPages.put( uri.toString(), Storage.mapper.readValue(response.body(), Map.class) );
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
        // Start prefetching of subsequent pages, if not already on the way:
        for (int i = 0; i < maxThreads; ++i) {
            prefetchPage(offset + 100 * i); // That the dump page size is 100 is an assumption, which MUST hold true for this to work correctly.
        }

        try {
            URI uri = new URI(System.getenv("EMMBASEURL")).resolve("full?selection=itemAndInstance:" + sigel + "&offset=" + offset);

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

    private static void startIfNotStarted() throws SQLException{
        try (HttpClient client = HttpClient.newHttpClient()) {

            URI uri = new URI(System.getenv("EMMBASEURL")).resolve("full?selection=itemAndInstance:" + sigel + "&offset=0");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            Map<?, ?> responseMap = Storage.mapper.readValue(response.body(), Map.class);
            if (responseMap.containsKey("startTime")) {
                String startTime = (String) responseMap.get("startTime");
                ZonedDateTime.parse(startTime); // parses correctly or throws

                Connection connection = Storage.getConnection();
                String dumpId = Storage.getState(DUMP_ID_KEY, connection);
                if (dumpId == null) { // This means no dump download is in progress. Time to start.
                    Storage.log("Starting download of EMM dump (sigel: " + sigel + ") with creation time: " + startTime);
                    Storage.clearState(OFFSET_KEY, connection);
                    Storage.writeState(DUMP_ID_KEY, startTime, connection);
                } else if (!dumpId.equals(startTime)) {
                    Storage.log("EMM dump is now stale, a restart is necessary. Will now download dump with creation time: " + startTime);
                    Storage.clearState(OFFSET_KEY, connection);
                    Storage.writeState(DUMP_ID_KEY, startTime, connection);
                }
                connection.commit();
            }

        } catch (URISyntaxException | IOException | InterruptedException e) {
            Storage.log("Failed to start dump download.", e);
        }
    }

}
