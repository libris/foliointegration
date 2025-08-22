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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Format {

    private static List<Map> getItems(String mainEntityUri, Connection connection) throws SQLException, IOException {
        String sql = """
                SELECT
                	entities.entity
                FROM
                	referenced_uris LEFT JOIN entities ON referenced_uris.entity_id = entities.id
                WHERE
                	referenced_uri = ?;
                """;

        List<Map> items = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, mainEntityUri);
            statement.execute();
            try (ResultSet resultSet = statement.getResultSet()) {
                if (resultSet.next()) {
                    String data = resultSet.getString(1);
                    Map holdingMap = Storage.mapper.readValue(data, Map.class);
                    items.add(holdingMap);
                }
            }
        }
        return items;
    }

    public static Map formatForFolio(Map originalRootHolding, Connection connection) throws SQLException, IOException {

        // Minimum required properties (by FOLIO): "instance" object with [ "source", "title", "instanceTypeId", "hrid" ] as determined by the json-schema.
        // See https://github.com/folio-org/mod-inventory-update/blob/master/ramls/inventory-record-set-with-hrids.json

        Map originalMainEntity = (Map) originalRootHolding.get("itemOf");

        List<Map> allItems = getItems( (String) originalMainEntity.get("@id"), connection);

        // Holdings
        List<Map> holdingRecords = new ArrayList<>();
        for (Map item : allItems) {
            String holdingHrid =  item.get("@id") + (String)((Map) item.get("heldBy")).get("@id"); // TEMP
            Map holdingRecord = Map.of(
                    "permanentLocationId", "b3826b33-be3b-49bd-b954-4b57bf84e70f",
                    "sourceId", "912ecb39-c577-4596-ad4b-0ed8dedc3a33", // TEMP MAKE CONFIGURABLE
                    "hrid", holdingHrid,
                    "items", new ArrayList<>()); // Todo: Examples ?
            holdingRecords.add(holdingRecord);
        }

        // Instance
        String title = "n/a";
        if (originalMainEntity.containsKey("hasTitle")) {
            List titles = (List) originalMainEntity.get("hasTitle");
            if (titles.size() > 0) {
                Map titleEntity = (Map) titles.get(0);
                if (titleEntity.containsKey("mainTitle"))
                    title = (String) titleEntity.get("mainTitle");
            }
        }
        String hrid = FolioWriting.lookupFolioHRID( (String) originalMainEntity.get("@id") ); // This is a BIG drain on write speed :(
        if (hrid == null) {
            // Not able to lookup HRID, make new?

            hrid = (String) originalMainEntity.get("@id"); // TEMP!
            Storage.log("Could not find HRID for " + originalMainEntity.get("@id") + " instead using: " + hrid);
        } else {
            Storage.log("Looked up HRID: " + hrid);
        }
        var converted = Map.of("instance",
                Map.of("source", "LIBRIS",
                        "hrid", hrid,
                        "instanceTypeId", "30fffe0e-e985-4144-b2e2-1e8179bdb41f", // = "unspecified" - for now.
                        "title", title,
                        "sourceUri", originalMainEntity.get("@id")
                ),
                "holdingsRecords", holdingRecords
        );

        return converted;
    }
}
