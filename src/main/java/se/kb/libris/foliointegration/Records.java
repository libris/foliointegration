package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Records {

    /**
     * Import a fetched root record (a kbv Item). The connection will be written to, but not
     * commited within this function. The function is reentrant.
     *
     * The record in question is assumed to be new, and not already exist.
     */
    public static void importRootRecord(List<?> graphList, Connection connection) {
        try {
            System.err.print(".");

            synchronized (connection) {
                try (PreparedStatement statement = connection.prepareStatement("INSERT INTO entities(entity) VALUES(?)")) {
                    statement.setString(1, Storage.mapper.writeValueAsString( graphList.get(1) ));
                    statement.execute();
                }

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

                System.err.println("Wrote entities(" + insertedRowId + ")");
            }
        } catch (SQLException | IOException e) {
            Storage.log("Could not write record. Fatal. ", e);
            System.exit(1);
        }
    }
}
