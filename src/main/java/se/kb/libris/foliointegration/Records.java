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
    private final static List<String> propertiesOfInterest = Arrays.asList("mainEntity", "instanceOf", "itemOf", "subject", "agent");

    /**
     * Import a fetched record. The connection will be written to, but not
     * commited within this function.
     */
    public static void writeRecord(Map<String, ?> mainEntity, Connection connection) {
        try {
            long insertedRowId = 0;
            String mainEntityId = (String) mainEntity.get("@id");
            // Write the entity itself
            try (PreparedStatement statement = connection.prepareStatement("INSERT OR REPLACE INTO entities(uri, entity, modified) VALUES(?, ?, ?)")) {
                statement.setString(1, mainEntityId);
                statement.setString(2, Storage.mapper.writeValueAsString(mainEntity));
                statement.setLong(3, new Date().toInstant().toEpochMilli());
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

            // Clear any existing URIs for this record
            try (PreparedStatement statement = connection.prepareStatement("DELETE FROM referenced_uris WHERE entity_id = ?")) {
                statement.setLong(1, insertedRowId);
                statement.execute();
            }

            // Write all URIs that the entity refers to (except itself)
            List<String> uris = collectUrisReferencedByThisRecord( mainEntity );
            for (String uri : uris) {
                if (!uri.equals(mainEntityId)) {
                    try (PreparedStatement statement = connection.prepareStatement("INSERT INTO referenced_uris(entity_id, referenced_uri) VALUES(?, ?)")) {
                        statement.setLong(1, insertedRowId);
                        statement.setString(2, uri);
                        statement.execute();
                    }
                }
            }

        } catch (SQLException | IOException e) {
            Storage.log("Could not write record. Fatal. ", e);
            System.exit(1);
        }
    }

    public static List<String> collectUrisReferencedByThisRecord(Object node) {
        var result = new ArrayList<String>();

        switch (node) {
            case List l: {
                for (Object o : l) {
                    result.addAll( collectUrisReferencedByThisRecord(o) );
                }
                break;
            }
            case Map m: {
                if (m.containsKey("@id")) {
                    result.add((String) m.get("@id"));
                }
                for (Object k : m.keySet()) {
                    if (propertiesOfInterest.contains(k)) {
                        result.addAll( collectUrisReferencedByThisRecord(m.get(k)) );
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

    public static void filterUrisWeAlreadyHave(List<String> uris, Connection connection) throws SQLException {
        var it = uris.iterator();
        while (it.hasNext()) {
            String dependencyToDownload = it.next();
            try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM entities WHERE uri = ?")) {
                statement.setString(1, dependencyToDownload);
                statement.execute();
                try (ResultSet resultSet = statement.getResultSet()) {
                    if (resultSet.next()) {
                        it.remove();
                    }
                }
            }
        }
    }

    public static void embellishWithLocalData(Object node, Set<String> cycleProtection, Connection connection) throws SQLException, IOException {
        switch (node) {
            case List l: {
                for (Object o : l) {
                    embellishWithLocalData(o, cycleProtection, connection);
                }
                break;
            }
            case Map m: {
                if (m.containsKey("@id") && m.size() == 1) {

                    if (cycleProtection.contains( m.get("@id") ))
                        return;
                    cycleProtection.add((String) m.get("@id"));

                    try (PreparedStatement statement = connection.prepareStatement("SELECT entity FROM entities WHERE uri = ?")) {
                        statement.setString(1, (String) m.get("@id"));
                        statement.execute();
                        try (ResultSet resultSet = statement.getResultSet()) {
                            if (resultSet.next()) {
                                var linkedEntity = Storage.mapper.readValue(resultSet.getString(1), Map.class);
                                m.clear();
                                m.putAll(linkedEntity);
                            }
                        }
                    }
                }
                for (Object k : m.keySet()) {
                    embellishWithLocalData(m.get(k), cycleProtection, connection);
                }
                break;
            }
            default: {
                break;
            }
        }
    }

    public static List<Map> downloadDependencies(List<String> uris) {
        var result = new ArrayList<Map>();

        for (String uri : uris) {
            String response = downloadJsonLdWithRetry(uri);
            if (response == null) {
                Storage.log("WARNING: Was unable to download a dependency: " + uri + " which may now be missing in the synced data.");
            } else {
                try {
                    Map dependency = Storage.mapper.readValue(response, Map.class);
                    if (dependency.containsKey("@graph")) {
                        List<Map> graphList = (List<Map>) dependency.get("@graph");
                        result.add(graphList.get(1));
                    }
                } catch (IOException ioe) {
                    Storage.log("Could not handle expected JSON.", ioe);
                }
            }
        }

        return result;
    }

    public static String downloadJsonLdWithRetry(String uri) {
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
