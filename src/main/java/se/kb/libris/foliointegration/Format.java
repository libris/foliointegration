package se.kb.libris.foliointegration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.schibsted.spt.data.jslt.Function;
import com.schibsted.spt.data.jslt.Parser;
import com.schibsted.spt.data.jslt.Expression;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

public class Format {

    // Initial lookup of FOLIO GUIDs for various things, done at startup.
    static Map<String, String> locationToGuid = new HashMap<>();
    static Map<String, String> instanceTypeToGuid = new HashMap<>();
    static Map<String, String> instanceNoteTypeToGuid = new HashMap<>();
    static Map<String, String> altTitleTypeToGuid = new HashMap<>();
    static Map<String, String> identifierTypeToGuid = new HashMap<>();
    static Map<String, String> instanceFormatToGuid = new HashMap<>();
    static String instanceJsltConversion = null;
    static String itemJsltConversion = null;
    static Instant lastJsltUpdate = Instant.ofEpochMilli(0);
    static {
        try {

            // locations
            String locations = FolioWriting.getFromFolio("/locations?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map locationsMap = Storage.mapper.readValue(locations, Map.class);
            List<Map> locationEntities = (List<Map>) locationsMap.get("locations");
            for (Map locationEntity : locationEntities) {
                locationToGuid.put((String)locationEntity.get("name"), (String)locationEntity.get("id")); // use code as key instead ?
            }

            // resource types
            String resourceTypesResponse = FolioWriting.getFromFolio("/instance-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map instanceTypesMap = Storage.mapper.readValue(resourceTypesResponse, Map.class);
            List<Map> instanceTypes = (List<Map>) instanceTypesMap.get("instanceTypes");
            for (Map instanceType : instanceTypes) {
                instanceTypeToGuid.put((String)instanceType.get("name"), (String)instanceType.get("id"));
            }

            // instance note types
            String instanceNotesResponse = FolioWriting.getFromFolio("instance-note-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map instanceNoteTypesMap = Storage.mapper.readValue(instanceNotesResponse, Map.class);
            List<Map> instanceNoteTypes = (List<Map>) instanceNoteTypesMap.get("instanceNoteTypes");
            for (Map instanceNoteType : instanceNoteTypes) {
                instanceNoteTypeToGuid.put((String)instanceNoteType.get("name"), (String)instanceNoteType.get("id"));
            }

            // alternative title types
            String altTypesResponse = FolioWriting.getFromFolio("alternative-title-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map altTitleTypesMap = Storage.mapper.readValue(altTypesResponse, Map.class);
            List<Map> altTitleTypes = (List<Map>) altTitleTypesMap.get("alternativeTitleTypes");
            for (Map altTitleType : altTitleTypes) {
                altTitleTypeToGuid.put((String)altTitleType.get("name"), (String)altTitleType.get("id"));
            }

            // identifier types
            String IdentifierTypesResponse = FolioWriting.getFromFolio("identifier-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map identifierTypesMap = Storage.mapper.readValue(IdentifierTypesResponse, Map.class);
            List<Map> identifierTypes = (List<Map>) identifierTypesMap.get("identifierTypes");
            for (Map identifierType : identifierTypes) {
                identifierTypeToGuid.put((String)identifierType.get("name"), (String)identifierType.get("id"));
            }

            // instance formats
            String instanceFormatesResponse = FolioWriting.getFromFolio("instance-formats?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map instanceFormatsMap = Storage.mapper.readValue(instanceFormatesResponse, Map.class);
            List<Map> instanceFormats = (List<Map>) instanceFormatsMap.get("instanceFormats");
            for (Map instanceFormat : instanceFormats) {
                instanceFormatToGuid.put((String)instanceFormat.get("name"), (String)instanceFormat.get("id"));
            }

            //Storage.log("** THIS: " + instanceFormatToGuid);

        } catch (IOException ioe) {
            Storage.log("Failed startup lookup of FOLIO GUIDs or other external resources.", ioe);
            System.exit(1);
        }

        if (!lookupJsltConversions()) {
            Storage.log("Failed startup lookup of FOLIO GUIDs or other external resources.");
            System.exit(1);
        }
    }

    public static boolean lookupJsltConversions() {

        Instant now = Instant.now();
        if (now.isBefore(lastJsltUpdate.plus(10, ChronoUnit.SECONDS)))
            return false;
        lastJsltUpdate = now;

        //String instanceJsltUrl = "https://git.kb.se/libris-folio/format-conversion/-/raw/develop/public/instance.jslt";
        //String itemJsltUrl = "https://git.kb.se/libris-folio/format-conversion/-/raw/develop/public/item.jslt";

        String instanceJsltUrl = System.getenv("INSTANCE_JSLT_URL");
        String itemJsltUrl = System.getenv("ITEM_JSLT_URL");
        boolean changed = false;

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {

            // Get instance conversion
            {
                URI uri = new URI(instanceJsltUrl);
                HttpGet request = new HttpGet(uri);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);
                ClassicHttpResponse response = httpClient.execute(request);
                String responseText = EntityUtils.toString(response.getEntity());
                if (response.getCode() != 200) {
                    Storage.log("Failed JSLT (instance) lookup: " + response);
                    return false;
                }

                if (responseText != null) {
                    if (instanceJsltConversion == null || !instanceJsltConversion.equals(responseText)) {
                        instanceJsltConversion = responseText;
                        Storage.log("Obtained a new set of conversion rules (instance).");
                    }
                }
            }

            // Get item conversion
            {
                URI uri = new URI(itemJsltUrl);
                HttpGet request = new HttpGet(uri);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);
                ClassicHttpResponse response = httpClient.execute(request);
                String responseText = EntityUtils.toString(response.getEntity());
                if (response.getCode() != 200) {
                    Storage.log("Failed JSLT (item) lookup: " + response);
                    return false;
                }

                if (responseText != null) {
                    if (itemJsltConversion == null || !itemJsltConversion.equals(responseText) ) {
                        itemJsltConversion = responseText;
                        Storage.log("Obtained a new set of conversion rules (item).");
                    }
                }
            }

        } catch (IOException | URISyntaxException | ParseException e) {
            Storage.log("Failed JSLT lookup.", e);
            return false;
        }

        return true;
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

    private static void jsltNoteTypeLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_NOTE_TYPE_GUID", instanceNoteTypeToGuid);
    }

    private static void jsltLocationLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_LOCATION_GUID", locationToGuid);
    }

    private static void jsltAltTitleLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_ALTTITLETYPE_GUID", altTitleTypeToGuid);
    }

    private static void jsltIdentifierLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_IDENTIFIER_TYPE_GUID", identifierTypeToGuid);
    }

    private static void jsltInstanceFormatLookup(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_INSTANCE_FORMAT_GUID", instanceFormatToGuid);
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
                String result = jsltFolioLookup(map.get(key), JSLTKey, lookupMap);
                if (result != null) {
                    guid = result;
                    it.remove();
                    removedKey = key;
                    //Storage.log(" ** REMOVING KEY: " + removedKey + " due to replacement with: " + guid);
                }
            }
            if (guid != null) {
                map.put(removedKey, guid);
                //Storage.log(" ** RE-ADDING KEY: " + removedKey + " with value: " + guid);
            }

        } else if (node instanceof List) {
            List list = (List) node;
            for (Object element : list) {
                jsltFolioLookup(element, JSLTKey, lookupMap);
            }
        }

        return null;
    }

    public static class ExposedDecodeFunction implements Function {
        public String getName() {
            return "urldecode";
        }

        public int getMinArguments() {
            return 1;
        }

        public int getMaxArguments() {
            return 1;
        }

        public JsonNode call(JsonNode input, JsonNode[] params) {
            String encoded = params[0].asText();
            return new TextNode(URLDecoder.decode(encoded, StandardCharsets.UTF_8));
        }
    }

    public static Map formatForFolio(Map originalRootHolding, Connection connection) throws SQLException, IOException {

        // Minimum required properties (by FOLIO): "instance" object with [ "source", "title", "instanceTypeId", "hrid" ] as determined by the json-schema.
        // See https://github.com/folio-org/mod-inventory-update/blob/master/ramls/inventory-record-set-with-hrids.json

        /*Expression instanceJSLT = Parser.compileString("""
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
                """);*/
        //Expression instanceJSLT = new Parser(new StringReader(instanceJsltConversion)).withObjectFilter(". != {} and . != []").compile(); // Leave nulls in place, but remove empty arrays/object
        Collection<Function> functions = Collections.singleton(new ExposedDecodeFunction());
        Expression instanceJSLT = Parser.compileString(instanceJsltConversion, functions);

        /*Expression holdingsJSLT = Parser.compileString("""
                let root = (.)
                
                [
                    for ( zip-with-index(.hasComponent) ) if ( not ( contains( "BESTÄLLD", .shelfMark.label ) ) )
                        {
                            "hrid" : get-key($root, "@id") + "-" + .index,
                            "permanentLocationId": { "__FOLIO_LOOKUP_LOCATION_GUID" : "Referensbiblioteket" },
                            "sourceId" : "7c764b4a-cce2-47ff-b64a-3aa3897a26a0"
                        }
                ]
                """);*/
        //Expression holdingsJSLT = new Parser(new StringReader(itemJsltConversion)).withObjectFilter(". != {} and . != []").compile(); // Leave nulls in place, but remove empty arrays/object
        Expression holdingsJSLT = Parser.compileString(itemJsltConversion, functions);

        Map originalMainEntity = (Map) originalRootHolding.get("itemOf");

        JsonNode instanceJsonNodeOriginal = Storage.mapper.valueToTree(originalMainEntity);
        JsonNode instanceJsonNodeTransformed = instanceJSLT.apply(instanceJsonNodeOriginal);
        Map jsltModifiedInstance = Storage.mapper.treeToValue(instanceJsonNodeTransformed, Map.class);
        jsltTypeLookup(jsltModifiedInstance);
        jsltNoteTypeLookup(jsltModifiedInstance);
        jsltAltTitleLookup(jsltModifiedInstance);
        jsltIdentifierLookup(jsltModifiedInstance);
        jsltInstanceFormatLookup(jsltModifiedInstance);

        List<Map> allItems = getItems( (String) originalMainEntity.get("@id"), connection);
        List folioItems = null;
        for (Map item : allItems) {
            // embed the instance, to make instance-info available during JSLT-transform.
            item.put("itemOf", originalMainEntity);

            JsonNode holdingsJsonNodeOriginal = Storage.mapper.valueToTree(item);
            JsonNode holdingsJsonNodeTransformed = holdingsJSLT.apply(holdingsJsonNodeOriginal);
            folioItems = Storage.mapper.treeToValue(holdingsJsonNodeTransformed, List.class);
        }
        jsltTypeLookup(folioItems);
        jsltNoteTypeLookup(folioItems);
        jsltAltTitleLookup(folioItems);
        jsltLocationLookup(folioItems);
        jsltIdentifierLookup(folioItems);
        jsltInstanceFormatLookup(folioItems);


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
