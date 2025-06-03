package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Records {

    /**
     * Import a fetched root record (a kbv Item). The connection will be written to, but not
     * commited within this function. The function is reentrant.
     */
    public static void writeNewRootRecord(List<?> graphList, Connection connection) {
        try {

            synchronized (connection) {
                // Write the entity itself
                try (PreparedStatement statement = connection.prepareStatement("INSERT INTO entities(entity) VALUES(?)")) {
                    statement.setString(1, Storage.mapper.writeValueAsString( graphList.get(1) ));
                    statement.execute();
                }

                // Get the ROWID of the written entity
                long insertedRowId = 0;
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

                // Write all URIs that the entity refers to
                List<String> uris = collectUrisReferencedByThisRecord( graphList.get(1) );
                for (String uri : uris) {
                    try (PreparedStatement statement = connection.prepareStatement("INSERT INTO uris(entity_id, uri) VALUES(?, ?)")) {
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

    private static List<String> collectUrisReferencedByThisRecord(Object node) {
        var result = new ArrayList<String>();

        switch (node) {
            case List l: {
                for (Object o : l) {
                    result.addAll( collectUrisReferencedByThisRecord(o) );
                }
                break;
            }
            case Map m: {
                if (m.containsKey("@id") && m.size() > 1) {
                    result.add((String) m.get("@id"));
                }
                for (Object k : m.keySet()) {
                    result.addAll( collectUrisReferencedByThisRecord(m.get(k)) );
                }
                break;
            }
            default: {
                break;
            }
        }

        return result;
    }
}
