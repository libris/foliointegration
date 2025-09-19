package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Format {

    // Initial lookup of FOLIO GUIDs for various things, done at startup.
    static Map<String, String> locationToGuid = new HashMap<>();
    static Map<String, String> instanceTypeToGuid = new HashMap<>();
    static {
        try {

            // locations
            String locations = FolioWriting.getFromFolio("/locations?limit=5000&query=cql.allRecords=1%20sortby%20name");
            Map locationsMap = Storage.mapper.readValue(locations, Map.class);
            List<Map> locationEntities = (List<Map>) locationsMap.get("locations");
            for (Map locationEntity : locationEntities) {
                locationToGuid.put((String)locationEntity.get("name"), (String)locationEntity.get("id")); // use code as key instead ?
            }

            // resource types
            String resourceTypesResponse = FolioWriting.getFromFolio("/instance-types?limit=5000&query=cql.allRecords=1%20sortby%20name");
            Map instanceTypesMap = Storage.mapper.readValue(resourceTypesResponse, Map.class);
            List<Map> instanceTypes = (List<Map>) instanceTypesMap.get("instanceTypes");
            for (Map instanceType : instanceTypes) {
                instanceTypeToGuid.put((String)instanceType.get("name"), (String)instanceType.get("id"));
            }

            //System.err.println(sourcessResponse);


        } catch (IOException ioe) {
            Storage.log("Failed startup lookup of FOLIO GUIDs", ioe);
            System.exit(1);
        }
    }

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

        //  ----  Holdings  ----
        List<Map> holdingRecords = new ArrayList<>();
        for (Map item : allItems) {
            String holdingHrid =  item.get("@id") + (String)((Map) item.get("heldBy")).get("@id"); // TEMP

            List folioItems = new ArrayList<>();
            if (item.containsKey("hasComponent")) {
                for (Map component : (List<Map>) item.get("hasComponent")) {
                    folioItems.add(Map.of("hrid", "" + item.get("@id") + component.hashCode() ));
                }
            } else {
                folioItems.add(Map.of("hrid", item.get("@id")));
            }

            Map holdingRecord = Map.of(
                    "permanentLocationId", "57246b20-76a7-4725-91c7-dea9f336de4f", // TEMP MAKE CONFIGURABLE
                    //"sourceId", "912ecb39-c577-4596-ad4b-0ed8dedc3a33", // TEMP
                    "sourceId", System.getenv("SOURCE_GUID"),
                    "hrid", holdingHrid,
                    "items", folioItems);
            holdingRecords.add(holdingRecord);
        }

        //  ----  Instance  ----

        // Titles
        String mainTitle = "n/a";
        List<String> alternativeTitles = new ArrayList<>();
        if (originalMainEntity.containsKey("hasTitle")) {
            List titles = (List) originalMainEntity.get("hasTitle");
            if (titles.size() > 0) {
                Map titleEntity = (Map) titles.get(0);
                if (titleEntity.containsKey("mainTitle"))
                    mainTitle = (String) titleEntity.get("mainTitle");
            }
            for (int i = 1; i < titles.size(); ++i) {
                Map titleEntity = (Map) titles.get(i);
                if (titleEntity.containsKey("mainTitle")) {
                    String altTitle = (String) titleEntity.get("mainTitle");
                    if (titleEntity.containsKey("subtitle")) {
                        altTitle += " : " + titleEntity.get("subtitle");
                    }
                    alternativeTitles.add(altTitle);
                }
            }
        }
        String hrid = (String) originalMainEntity.get("@id"); // This HRID will be looked up and replaced by the folio writing code before actual writing.

        String instanceTypeId = instanceTypeToGuid.get("unspecified"); // for now, match actual types later.

        // Map.of is pretty, but produces immutable maps, which we cannot have here (we must replace the HRID later).
        Map instance = new HashMap();
        instance.put("source", "LIBRIS");
        instance.put("hrid", hrid);
        instance.put("instanceTypeId", instanceTypeId);
        instance.put("title", mainTitle);
        instance.put("indexTitle", mainTitle);
        instance.put("sourceUri", originalMainEntity.get("@id"));
        Map converted = new HashMap();
        converted.put("instance", instance);
        //converted.put("holdingsRecords", holdingRecords);
        converted.put("holdingsRecords", new ArrayList<Map>()); // TEMP

        return converted;
    }
}
