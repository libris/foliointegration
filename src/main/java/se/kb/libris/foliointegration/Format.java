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

            //System.err.println( " ******** LOCATIONS: " + locationToGuid);


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

    private static void jsltTypeLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_TYPE_GUID", instanceTypeToGuid);
    }

    private static void jsltLocationLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_LOCATION_GUID", locationToGuid);
    }

    private static String jsltFolioLookup(Object node, String JSLTKey, Map<String, String> lookupMap) {
        if (node instanceof Map) {
            Map map = (Map) node;

            if (map.containsKey(JSLTKey)) {
                return lookupMap.get( (String) map.get(JSLTKey) );
            }

            String removedKey = null;
            String guid = null;
            Iterator it = map.keySet().iterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                guid = jsltFolioLookup(map.get(key), JSLTKey, lookupMap);
                if (guid != null) {
                    it.remove();
                    removedKey = key;
                }
            }
            if (guid != null) {
                map.put(removedKey, guid);
            }

        } else if (node instanceof List) {
            List list = (List) node;
            for (Object element : list) {
                jsltFolioLookup(element, JSLTKey, lookupMap);
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
                            "hrid" : get-key($root, "@id") + "-" + .index,
                            "location": { "__FOLIO_LOOKUP_LOCATION_GUID" : "Referensbiblioteket" },
                            "shelfMark" : .value.shelfMark
                        }
                ]
                """);

        Map originalMainEntity = (Map) originalRootHolding.get("itemOf");

        JsonNode instanceJsonNodeOriginal = Storage.mapper.valueToTree(originalMainEntity);
        JsonNode instanceJsonNodeTransformed = instanceJSLT.apply(instanceJsonNodeOriginal);
        Map jsltModifiedInstance = Storage.mapper.treeToValue(instanceJsonNodeTransformed, Map.class);
        jsltTypeLookup(jsltModifiedInstance);

        List<Map> allItems = getItems( (String) originalMainEntity.get("@id"), connection);
        List folioItems = null;
        for (Map item : allItems) {
            // embed the instance, to make instance-info available during JSLT-transform.
            item.put("itemOf", originalMainEntity);

            JsonNode holdingsJsonNodeOriginal = Storage.mapper.valueToTree(item);
            JsonNode holdingsJsonNodeTransformed = holdingsJSLT.apply(holdingsJsonNodeOriginal);
            folioItems = Storage.mapper.treeToValue(holdingsJsonNodeTransformed, List.class);
        }
        jsltLocationLookup(folioItems);


        Map converted = new HashMap();
        converted.put("instance", jsltModifiedInstance);

        // No holdings? Empty list, not null!
        if (folioItems == null)
            folioItems = new ArrayList();
        
        converted.put("holdingsRecords", folioItems);

        //Storage.log(" ** CONVERTED INTO: " + converted);

        return converted;
    }
}
