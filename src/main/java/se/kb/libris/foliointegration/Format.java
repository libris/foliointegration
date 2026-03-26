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
import java.util.function.Consumer;

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

    static String instanceJsltConversion = null;
    static String holdingJsltConversion = null;
    static String itemJsltConversion = null;
    static Instant lastJsltUpdate = Instant.ofEpochMilli(0);

    private static List<Consumer<Object>> lookupFunctions = new ArrayList<>();
    static {
        try {

            populateStandardLookup(
                    "locations?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "locations",
                    "__FOLIO_LOOKUP_LOCATION_GUID",
                    false
            );

            populateStandardLookup(
                    "instance-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "instanceTypes",
                    "__FOLIO_LOOKUP_TYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "instance-note-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "instanceNoteTypes",
                    "__FOLIO_LOOKUP_NOTE_TYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "alternative-title-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "alternativeTitleTypes",
                    "__FOLIO_LOOKUP_ALTTITLETYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "identifier-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "identifierTypes",
                    "__FOLIO_LOOKUP_IDENTIFIER_TYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "instance-formats?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "instanceFormats",
                    "__FOLIO_LOOKUP_INSTANCE_FORMAT_GUID",
                    false
            );

            populateStandardLookup(
                    "subject-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "subjectTypes",
                    "__FOLIO_LOOKUP_SUBJECT_TYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "subject-sources?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "subjectSources",
                    "__FOLIO_LOOKUP_SUBJECT_SOURCE_GUID",
                    false
            );

            populateStandardLookup(
                    "classification-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "classificationTypes",
                    "__FOLIO_LOOKUP_CLASSIFICATION_TYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "electronic-access-relationships?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "electronicAccessRelationships",
                    "__FOLIO_LOOKUP_ELECTRONIC_ACCESS_RELATIONSHIP_GUID",
                    false
            );

            populateStandardLookup(
                    "modes-of-issuance?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "issuanceModes",
                    "__FOLIO_LOOKUP_MODE_OF_ISSUANCE_GUID",
                    false
            );

            populateStandardLookup(
                    "contributor-name-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "contributorNameTypes",
                    "__FOLIO_LOOKUP_CONTRIBUTOR_NAME_TYPE_GUID",
                    false
            );

            populateStandardLookup(
                    "statistical-codes?query=cql.allRecords=1%20sortby%20code&limit=5000",
                    "statisticalCodes",
                    "__FOLIO_LOOKUP_STATISTICAL_CODES",
                    false
            );

            populateStandardLookup(
                    "call-number-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "callNumberTypes",
                    "__FOLIO_LOOKUP_CALL_NUMBER_TYPES",
                    false
            );

            populateStandardLookup(
                    "loan-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "loantypes",
                    "__FOLIO_LOOKUP_LOAN_TYPES",
                    false
            );

            populateStandardLookup(
                    "material-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "mtypes",
                    "__FOLIO_LOOKUP_MATERIAL_TYPES",
                    false
            );

            populateStandardLookup(
                    "item-note-types?query=cql.allRecords=1%20sortby%20name&limit=5000",
                    "itemNoteTypes",
                    "__FOLIO_LOOKUP_ITEM_NOTE_TYPES",
                    false
            );

        } catch (IOException ioe) {
            Storage.log("Failed startup lookup of FOLIO GUIDs or other external resources.", ioe);
            System.exit(1);
        }

        if (!lookupJsltConversions()) {
            Storage.log("Failed startup lookup of FOLIO GUIDs or other external resources.");
            System.exit(1);
        }
    }

    private static void populateStandardLookup(String query, String listName, String lookupCode, boolean verbose) throws IOException {

        Map<String, String> nameToGuidResult = new HashMap<>();
        String response = FolioWriting.getFromFolio(query);

        if (verbose) {
            Storage.log("Created a lookup out of:\n" + response);
        }

        Map responseMap = Storage.mapper.readValue(response, Map.class);
        List<Map> elements = (List<Map>) responseMap.get(listName);
        for (Map element : elements) {
            nameToGuidResult.put((String)element.get("name"), (String)element.get("id"));
        }

        lookupFunctions.add( o -> jsltFolioLookup(o, lookupCode, nameToGuidResult) );
        if (verbose) {
            Storage.log("into:\n" + nameToGuidResult);
        }
    }

    private static void jsltFolioLookups(Object node) {
        for (var c : lookupFunctions) {
            c.accept(node);
        }
    }

    public static boolean lookupJsltConversions() {

        Instant now = Instant.now();
        if (now.isBefore(lastJsltUpdate.plus(10, ChronoUnit.SECONDS)))
            return false;
        lastJsltUpdate = now;

        String instanceJsltUrl = System.getenv("INSTANCE_JSLT_URL");
        String holdingJsltUrl = System.getenv("HOLDING_JSLT_URL");
        String itemJsltUrl = System.getenv("ITEM_JSLT_URL");

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

            // Get holding conversion
            {
                URI uri = new URI(holdingJsltUrl);
                HttpGet request = new HttpGet(uri);
                RequestConfig config = RequestConfig.custom()
                        .setConnectionRequestTimeout(Timeout.ofSeconds(5)).setConnectionKeepAlive(TimeValue.ofSeconds(5)).build();
                request.setConfig(config);
                ClassicHttpResponse response = httpClient.execute(request);
                String responseText = EntityUtils.toString(response.getEntity());
                if (response.getCode() != 200) {
                    Storage.log("Failed JSLT (holding) lookup: " + response);
                    return false;
                }

                if (responseText != null) {
                    if (holdingJsltConversion == null || !holdingJsltConversion.equals(responseText)) {
                        holdingJsltConversion = responseText;
                        Storage.log("Obtained a new set of conversion rules (holding).");
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

    private static List<Map> getHoldings(String mainEntityUri, Connection connection) throws SQLException, IOException {
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
            String guid = null;
            Iterator it =  list.iterator();
            while (it.hasNext()) {
                Object element = it.next();

                String result = jsltFolioLookup(element, JSLTKey, lookupMap);
                if (result != null) {
                    guid = result;
                    it.remove();
                }
            }
            if (guid != null) {
                list.add(guid);
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

        //Expression instanceJSLT = new Parser(new StringReader(instanceJsltConversion)).withObjectFilter(". != {} and . != []").compile(); // Leave nulls in place, but remove empty arrays/object
        Collection<Function> functions = Collections.singleton(new ExposedDecodeFunction());
        Expression instanceJSLT = Parser.compileString(instanceJsltConversion, functions);
        Expression holdingJSLT = Parser.compileString(holdingJsltConversion, functions);
        Expression itemsJSLT = Parser.compileString(itemJsltConversion, functions);

        Map originalMainEntity = (Map) originalRootHolding.get("itemOf");

        JsonNode instanceJsonNodeOriginal = Storage.mapper.valueToTree(originalMainEntity);
        JsonNode instanceJsonNodeTransformed = instanceJSLT.apply(instanceJsonNodeOriginal);
        Map jsltModifiedInstance = Storage.mapper.treeToValue(instanceJsonNodeTransformed, Map.class);
        jsltFolioLookups(jsltModifiedInstance);


        List<Map> allLibrisHoldings = getHoldings( (String) originalMainEntity.get("@id"), connection);
        List<Map> folioHoldings = new ArrayList<>(allLibrisHoldings.size());

        for (Map item : allLibrisHoldings) {
            // embed the instance, to make instance-info available during JSLT-transform.
            item.put("itemOf", originalMainEntity);

            JsonNode holdingsJsonNodeOriginal = Storage.mapper.valueToTree(item);
            JsonNode holdingsJsonNodeTransformed = holdingJSLT.apply(holdingsJsonNodeOriginal);
            Map folioHolding = Storage.mapper.treeToValue(holdingsJsonNodeTransformed, Map.class);

            List<Map> folioItems = new ArrayList<>();
            if (item.containsKey("hasComponent")) {
                JsonNode itemsJsonNodeTransformed = itemsJSLT.apply(holdingsJsonNodeOriginal);
                //Storage.log(Storage.mapper.writeValueAsString(itemsJsonNodeTransformed));
                folioItems.addAll(Storage.mapper.treeToValue(itemsJsonNodeTransformed, List.class));
                folioHolding.put("items", folioItems);
            }

            folioHoldings.add( folioHolding );
        }
        jsltFolioLookups(folioHoldings);

        Map converted = new HashMap();
        converted.put("instance", jsltModifiedInstance);

        // No holdings? Empty list, not null!
        if (folioHoldings == null)
            folioHoldings = new ArrayList();

        converted.put("holdingsRecords", folioHoldings);

        //Storage.log(" ** CONVERTED INTO: " + converted);

        return converted;
    }
}
