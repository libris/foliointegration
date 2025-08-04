package se.kb.libris.foliointegration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Format {
    public static Map formatForFolio(Map originalRootHolding) {

        // Minimum required properties (by FOLIO): "instance" object with [ "source", "title", "instanceTypeId", "hrid" ] as determined by the json-schema.
        // See https://github.com/folio-org/mod-inventory-update/blob/master/ramls/inventory-record-set-with-hrids.json

        Map originalMainEntity = (Map) originalRootHolding.get("itemOf");


        String title = "n/a";
        if (originalMainEntity.containsKey("hasTitle")) {
            List titles = (List) originalMainEntity.get("hasTitle");
            if (titles.size() > 0) {
                Map titleEntity = (Map) titles.get(0);
                if (titleEntity.containsKey("mainTitle"))
                    title = (String) titleEntity.get("mainTitle");
            }
        }

        var converted = Map.of("instance",
                Map.of("source", "LIBRIS",
                        "hrid", originalRootHolding.get("@id"),
                        "instanceTypeId", "30fffe0e-e985-4144-b2e2-1e8179bdb41f", // = "unspecified" - for now.
                        "title", title
                ),
                "holdingsRecords", new ArrayList<>() // we probably don't want this ?
        );

        return converted;
    }
}
