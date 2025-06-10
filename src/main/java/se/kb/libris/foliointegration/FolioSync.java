package se.kb.libris.foliointegration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class FolioSync {

    public final static String SYNCED_UNTIL_KEY = "FOLIOSyncStateSyncedUntil";

    public static void run() throws SQLException {
        Connection connection = Storage.getConnection();
        long syncedUntil = Long.parseLong( Storage.getState(SYNCED_UNTIL_KEY, connection) );

        try (PreparedStatement statement = connection.prepareStatement("SELECT entity, modified FROM entities WHERE modified > ? ORDER BY modified LIMIT 50")) {
            statement.setLong(1, syncedUntil);
            statement.execute();
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    String data = resultSet.getString(1);
                    Long modified = resultSet.getLong(2);
                }
            }
        }
        
    }
}
