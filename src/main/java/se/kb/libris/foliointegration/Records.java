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
import java.util.*;

public class Records {

    /**
     * Import a fetched root record (a kbv Item). The connection will be written to, but not
     * commited within this function.
     */
    public static void writeNewRecord(List<?> graphList, Connection connection) {
        Map<String, ?> mainEntity = (Map<String, ?>) graphList.get(1);
        try {
            long insertedRowId = 0;
            // Write the entity itself
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO entities(uri, entity) VALUES(?, ?)")) {
                statement.setString(1, (String) mainEntity.get("@id"));
                statement.setString(2, Storage.mapper.writeValueAsString(mainEntity));
                statement.execute();
            }

            // Get the ROWID of the written entity
            try (PreparedStatement statement = connection.prepareStatement("SELECT last_insert_rowid()")) {
                statement.execute();
                try (ResultSet resultSet = statement.getResultSet()) {
                    if (!resultSet.next()) {
                        Storage.logWithCallstack("SQLITE last_insert_rowid() failure. Fatal.");
                        System.exit(1);
                    }
                    insertedRowId = resultSet.getLong(1);
                }
            }

            // Write all URIs that the entity refers to
            List<String> uris = collectUrisReferencedByThisRecord( mainEntity );
            for (String uri : uris) {
                try (PreparedStatement statement = connection.prepareStatement("INSERT INTO uris(entity_id, uri) VALUES(?, ?)")) {
                    statement.setLong(1, insertedRowId);
                    statement.setString(2, uri);
                    statement.execute();
                }
            }

        } catch (SQLException | IOException e) {
            Storage.log("Could not write record. Fatal. ", e);
            System.exit(1);
        }
    }

    private static List<String> collectUrisReferencedByThisRecord(Object node) {
        var result = new ArrayList<String>();

        switch (node) {
            case List l: {
                for (Object o : l) {
                    result.addAll( collectUrisReferencedByThisRecord(o) );
                }
                break;
            }
            case Map m: {
                if (m.containsKey("@id") && m.size() > 1) {
                    result.add((String) m.get("@id"));
                }
                for (Object k : m.keySet()) {
                    result.addAll( collectUrisReferencedByThisRecord(m.get(k)) );
                }
                break;
            }
            default: {
                break;
            }
        }

        return result;
    }

    private final static List<String> propertiesOfInterest = Arrays.asList("mainEntity", "itemOf", "subject", "agent");

    public static List<List<?>> downloadDependencies(Object node) {
        var result = new ArrayList<List<?>>();

        switch (node) {
            case List l: {
                for (Object o : l) {
                    result.addAll( downloadDependencies(o) );
                }
                break;
            }
            case Map m: {
                if (m.containsKey("@id") && m.size() == 1) {
                    String response = downloadJsonLdWithRetry((String) m.get("@id"));
                    if (response == null) {
                        Storage.log("WARNING: Was unable to download a dependency: " + m.get("@id") + " which may now be missing in the synced data.");
                    } else {
                        try {
                            Map dependency = Storage.mapper.readValue(response, Map.class);
                            if (dependency.containsKey("@graph")) {
                                //writeNewRootRecord((List<?>) dependency.get("@graph"), connection);
                                result.add((List<?>) dependency.get("@graph"));
                            }
                        } catch (IOException ioe) {
                            Storage.log("Could not handle expected JSON.", ioe);
                        }
                    }
                }
                for (Object k : m.keySet()) {
                    if (propertiesOfInterest.contains(k)) {
                        result.addAll( downloadDependencies(m.get(k)) );
                    }
                }
                break;
            }
            default: {
                break;
            }
        }

        return result;
    }

    private static String downloadJsonLdWithRetry(String uri) {
        for (int i = 0; i < 5; ++i) {
            try (HttpClient client = HttpClient.newHttpClient()) {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(new URI(uri))
                        .header("accept", "application/json+ld")
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                return response.body();
            } catch (IOException | URISyntaxException | InterruptedException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e2) {
                    // ignore
                }
            }
        }

        return null;
    }

}
