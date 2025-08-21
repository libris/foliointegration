package se.kb.libris.foliointegration;

import java.io.IOException;
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
        //System.err.println("Holdings to attach: " + allItems);

        String title = "n/a";
        if (originalMainEntity.containsKey("hasTitle")) {
            List titles = (List) originalMainEntity.get("hasTitle");
            if (titles.size() > 0) {
                Map titleEntity = (Map) titles.get(0);
                if (titleEntity.containsKey("mainTitle"))
                    title = (String) titleEntity.get("mainTitle");
            }
        }

        List<Map> holdingRecords = new ArrayList<>();
        for (Map item : allItems) {
            String hrid =  item.get("@id") + (String)((Map) item.get("heldBy")).get("@id");

            Map holdingRecord = Map.of(
                    //"source", "LIBRIS",
                    //"sourceUri", item.get("@id"),
                    "permanentLocationId", "b3826b33-be3b-49bd-b954-4b57bf84e70f",
                    "sourceId", "912ecb39-c577-4596-ad4b-0ed8dedc3a33", // TEMP MAKE CONFIGURABLE
                    "hrid", hrid,
                    "items", new ArrayList<>()); // Todo: Examples ?
            holdingRecords.add(holdingRecord);
        }

        var converted = Map.of("instance",
                Map.of("source", "LIBRIS",
                        "hrid", originalMainEntity.get("@id"), // TEMP! GET FROM ALEPH IF IT EXISTS, OR MINT NEW IN YET UNDECIDED WAY!
                        "instanceTypeId", "30fffe0e-e985-4144-b2e2-1e8179bdb41f", // = "unspecified" - for now.
                        "title", title,
                        "sourceUri", originalMainEntity.get("@id")
                ),
                "holdingsRecords", holdingRecords
        );

        return converted;
    }
}
