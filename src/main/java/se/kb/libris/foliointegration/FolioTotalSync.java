package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class FolioTotalSync {

    public final static String SYNCED_TO_ID_KEY = "FOLIOTotalSyncStateSyncedUpToID";

    public static boolean run() throws SQLException, IOException, InterruptedException {
        Connection connection = Storage.getConnection();

        String syncedToID = Storage.getState(SYNCED_TO_ID_KEY, connection);
        if (syncedToID == null) {
            return false; // No total sync in progress, nothing to do.
        }

        long syncedTo = Long.parseLong(syncedToID);

        List<Long> ids = new ArrayList<>(2000);
        // Select only Items.
        try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM entities WHERE id > ? AND json_extract(entity, '$.itemOf') IS NOT NULL ORDER BY id LIMIT 200")) {
            statement.setLong(1, syncedTo);
            statement.execute();
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    Long id = resultSet.getLong(1);
                    ids.add(id);
                }
            }
        }

        if (ids.isEmpty()) {
            Storage.clearState(SYNCED_TO_ID_KEY, connection);
            Storage.log("Total sync (instances) to FOLIO completed.");
            return false;
        }

        for (Long id : ids) {
            FolioSync.considerForExport(id, new HashSet<>(), connection);
        }

        if (FolioWriting.finalizePendingWrites(connection)) {
            Storage.writeState(SYNCED_TO_ID_KEY, "" + ids.getLast(), connection);
            Storage.log("Total sync (instances) to FOLIO now at: " + ids.getLast());
        }

        return !ids.isEmpty();
    }
}
