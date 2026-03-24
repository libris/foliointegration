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
    static Map<String, String> subjectTypeToGuid = new HashMap<>();
    static Map<String, String> subjectSourceToGuid = new HashMap<>();
    static Map<String, String> classificationTypeToGuid = new HashMap<>();
    static Map<String, String> electronicAccessRelationshipToGuid = new HashMap<>();
    static Map<String, String> modeOfIssuanceToGuid = new HashMap<>();
    static Map<String, String> contributorNameTypeToGuid = new HashMap<>();

    static String instanceJsltConversion = null;
    static String holdingJsltConversion = null;
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

            // subject type
            String subjectTypesResponse = FolioWriting.getFromFolio("subject-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map subjectTypesMap = Storage.mapper.readValue(subjectTypesResponse, Map.class);
            List<Map> subjectTypes = (List<Map>) subjectTypesMap.get("subjectTypes");
            for (Map subjectType : subjectTypes) {
                subjectTypeToGuid.put((String)subjectType.get("name"), (String)subjectType.get("id"));
            }

            // subject source
            String subjectSourcesResponse = FolioWriting.getFromFolio("subject-sources?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map subjectSourcesMap = Storage.mapper.readValue(subjectSourcesResponse, Map.class);
            List<Map> subjectSources = (List<Map>) subjectSourcesMap.get("subjectSources");
            for (Map subjectSource : subjectSources) {
                subjectSourceToGuid.put((String)subjectSource.get("name"), (String)subjectSource.get("id"));
            }

            // classification types
            String classificationTypesResponse = FolioWriting.getFromFolio("classification-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map classificationTypesMap = Storage.mapper.readValue(classificationTypesResponse, Map.class);
            List<Map> classificationTypes = (List<Map>) classificationTypesMap.get("classificationTypes");
            for (Map classificationType : classificationTypes) {
                classificationTypeToGuid.put((String)classificationType.get("name"), (String)classificationType.get("id"));
            }

            String electronicAccessRelationshipsResponse = FolioWriting.getFromFolio("electronic-access-relationships?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map electronicAccessRelationshipsMap = Storage.mapper.readValue(electronicAccessRelationshipsResponse, Map.class);
            List<Map> electronicAccessRelationships = (List<Map>) electronicAccessRelationshipsMap.get("electronicAccessRelationships");
            for (Map electronicAccessRelationship : electronicAccessRelationships) {
                electronicAccessRelationshipToGuid.put((String)electronicAccessRelationship.get("name"), (String)electronicAccessRelationship.get("id"));
            }

            String modesOfIssuanceResponse = FolioWriting.getFromFolio("modes-of-issuance?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map modesOfIssuanceMap = Storage.mapper.readValue(modesOfIssuanceResponse, Map.class);
            List<Map> modesOfIssuance = (List<Map>) modesOfIssuanceMap.get("issuanceModes");
            for (Map modeOfIssuance : modesOfIssuance) {
                modeOfIssuanceToGuid.put((String)modeOfIssuance.get("name"), (String)modeOfIssuance.get("id"));
            }

            String contributorNameTypesResponse = FolioWriting.getFromFolio("contributor-name-types?query=cql.allRecords=1%20sortby%20name&limit=5000");
            Map contributorNameTypesMap = Storage.mapper.readValue(contributorNameTypesResponse, Map.class);
            List<Map> contributorNameTypes = (List<Map>) contributorNameTypesMap.get("contributorNameTypes");
            for (Map contributorNameType : contributorNameTypes) {
                contributorNameTypeToGuid.put((String)contributorNameType.get("name"), (String)contributorNameType.get("id"));
            }

            //Storage.log("** THIS: " + contributorNameTypeToGuid);

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

    private static void jsltFolioLookups(Object node) {
        jsltFolioLookup(node, "__FOLIO_LOOKUP_TYPE_GUID", instanceTypeToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_NOTE_TYPE_GUID", instanceNoteTypeToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_LOCATION_GUID", locationToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_ALTTITLETYPE_GUID", altTitleTypeToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_IDENTIFIER_TYPE_GUID", identifierTypeToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_INSTANCE_FORMAT_GUID", instanceFormatToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_SUBJECT_TYPE_GUID", subjectTypeToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_SUBJECT_SOURCE_GUID", subjectSourceToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_CLASSIFICATION_TYPE_GUID", classificationTypeToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_ELECTRONIC_ACCESS_RELATIONSHIP_GUID", electronicAccessRelationshipToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_MODE_OF_ISSUANCE_GUID", modeOfIssuanceToGuid);
        jsltFolioLookup(node, "__FOLIO_LOOKUP_CONTRIBUTOR_NAME_TYPE_GUID", contributorNameTypeToGuid);
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
