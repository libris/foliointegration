package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.schibsted.spt.data.jslt.Parser;
import com.schibsted.spt.data.jslt.Expression;

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

    private static String doTypeLookup(Object node) {
        if (node instanceof Map) {
            Map map = (Map) node;

            if (map.containsKey("__FOLIO_LOOKUP_TYPE_GUID")) {
                return instanceTypeToGuid.get( (String) map.get("__FOLIO_LOOKUP_TYPE_GUID") );
            }

            String removedKey = null;
            String guid = null;
            Iterator it = map.keySet().iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                guid = doTypeLookup(map.get(key));
                if (guid != null) {
                    it.remove();
                    removedKey = key;
                }
            }
            if (removedKey != null) {
                map.put(removedKey, guid);
            }

        } else if (node instanceof List) {
            List list = (List) node;
            for (Object element : list) {
                doTypeLookup(element);
            }
        }

        return null;
    }

    public static Map formatForFolio(Map originalRootHolding, Connection connection) throws SQLException, IOException {

        // Minimum required properties (by FOLIO): "instance" object with [ "source", "title", "instanceTypeId", "hrid" ] as determined by the json-schema.
        // See https://github.com/folio-org/mod-inventory-update/blob/master/ramls/inventory-record-set-with-hrids.json

        Expression instanceJSLT = Parser.compileString("""
                let titles = .hasTitle
                let mainTitles = [ for ($titles) if (.mainTitle) .mainTitle ]
                let mainTitle = $mainTitles[0]
                
                {
                    "source" : "LIBRIS",
                    "hrid" : .meta.controlNumber,
                    "title" : $mainTitle,
                    "sourceUri" : get-key(., "@id"),
                    "instanceTypeId": { "__FOLIO_LOOKUP_TYPE_GUID" : "unspecified" }
                }
                """);

        Expression holdingsJSLT = Parser.compileString("""
                let root = (.)
                
                [
                    for ( zip-with-index(.hasComponent) ) if ( not ( contains( "BESTÄLLD", .shelfMark.label ) ) )
                        {
                            "hrid" : $root.itemOf.meta.controlNumber + "-" + .index,
                            "shelfMark" : .value.shelfMark
                        }
                ]
                """);

        Map originalMainEntity = (Map) originalRootHolding.get("itemOf");

        JsonNode instanceJsonNodeOriginal = Storage.mapper.valueToTree(originalMainEntity);
        JsonNode instanceJsonNodeTransformed = instanceJSLT.apply(instanceJsonNodeOriginal);
        Map jsltModifiedInstance = Storage.mapper.treeToValue(instanceJsonNodeTransformed, Map.class);
        doTypeLookup(jsltModifiedInstance);

        //Storage.log(" ** CONVERTED INTO: " + jsltModifiedInstance);

        List<Map> allItems = getItems( (String) originalMainEntity.get("@id"), connection);
        List folioItems = null;
        for (Map item : allItems) {
            // embed the instance, to make instance-info available during JSLT-transform.
            item.put("itemOf", originalMainEntity);

            JsonNode holdingsJsonNodeOriginal = Storage.mapper.valueToTree(item);
            JsonNode holdingsJsonNodeTransformed = holdingsJSLT.apply(holdingsJsonNodeOriginal);
            folioItems = Storage.mapper.treeToValue(holdingsJsonNodeTransformed, List.class);
        }


        Map converted = new HashMap();
        converted.put("instance", jsltModifiedInstance);
        converted.put("holdingsRecords", folioItems);

        return converted;
    }
}
