package se.kb.libris.foliointegration;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Records {
    private final static List<String> propertiesOfInterest = Arrays.asList("mainEntity", "instanceOf", "itemOf", "subject", "agent", "contribution", "category");

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
            Set<String> uris = collectUrisReferencedByThisRecord( mainEntity );
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

    public static Set<String> collectUrisReferencedByThisRecord(Object node) {
        var result = new HashSet<String>();

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

    public static void filterUrisWeAlreadyHave(Set<String> uris, Connection connection) throws SQLException {
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

                    //Storage.log(" embedding (if exists): " + m.get("@id") + " cycleprot: " + cycleProtection);

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
                    if (!k.equals("narrower") && !k.equals("broader") && !k.equals("@reverse"))
                        embellishWithLocalData(m.get(k), cycleProtection, connection);
                }
                break;
            }
            default: {
                break;
            }
        }
    }

    public static List<Map> downloadDependencies(Set<String> urisToDownload, Set<String> cycleProtection, Connection connection) {
        var result = new ArrayList<Map>();

        String baseUrl = System.getenv("EMM_BASE_URL");

        for (String uri : urisToDownload) {

            // Don't reach to id.kb.se PROD even though that's what the links say. :( Kinda hacky!
            if ( uri.startsWith("https://id.kb.se") ) {
                if (baseUrl.startsWith("https://libris-qa.kb.se"))
                    uri = uri.replace("https://id.kb.se", "https://id-qa.kb.se");
                else if (baseUrl.startsWith("https://libris-dev.kb.se"))
                    uri = uri.replace("https://id.kb.se", "https://id-dev.kb.se");
            }

            if (cycleProtection.contains(uri))
                continue;
            cycleProtection.add(uri);

            String response = downloadJsonLdWithRetry(uri);
            if (response == null) {
                Storage.log("WARNING: Was unable to download a dependency: " + uri + " which may now be missing in the synced data.");
            } else {
                try {
                    Map dependency = Storage.mapper.readValue(response, Map.class);
                    if (dependency.containsKey("@graph")) {
                        List<Map> graphList = (List<Map>) dependency.get("@graph");

                        Map recordEntity = graphList.get(0);
                        Map mainEntity = graphList.get(1);
                        if (recordEntity.containsKey("controlNumber")) {
                            mainEntity.put( "meta", recordEntity );
                        }
                        result.add(mainEntity);

                        Set<String> nextOrderDependencies = collectUrisReferencedByThisRecord(graphList.get(1));
                        filterUrisWeAlreadyHave(nextOrderDependencies, connection);
                        result.addAll( downloadDependencies(nextOrderDependencies, cycleProtection, connection) );
                    }
                } catch (IOException ioe) {
                    Storage.log("Could not handle expected JSON from: " + uri + " [which looks like]: " + response, ioe);
                } catch (SQLException se) {
                    Storage.log("Failed to check for existence higher order dependencies.", se);
                }
            }
        }

        return result;
    }

    public static String downloadJsonLdWithRetry(String uri) {
        for (int i = 0; i < 5; ++i) {
            try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
                HttpGet request = new HttpGet(uri);
                URI uriWithParam = new URIBuilder(request.getUri() )
                        .addParameter("computedLabel", "sv")
                        .addParameter("framed", "false")
                        .addParameter("embellished", "false")
                        .build();
                request.setUri(uriWithParam);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);
                request.setHeader("accept", "application/json+ld");
                ClassicHttpResponse response = httpClient.execute(request);

                boolean isJsonld = false;
                for (Header header : response.getHeaders("Content-Type")) {
                    if (header.getValue().contains("application/ld+json") || header.getValue().contains("application/json"))
                        isJsonld = true;
                }

                if (isJsonld)
                    return EntityUtils.toString(response.getEntity());
                else {
                    Storage.log("Asked for JSONLD on " + uri + " but got other content-type anyway.");
                    return null;
                }

            } catch (IOException | ParseException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e2) {
                    // ignore
                }
            } catch (URISyntaxException e) {
                Storage.log("WARNING: Bad URI: " + uri, e);
            }
        }

        return null;
    }

}
