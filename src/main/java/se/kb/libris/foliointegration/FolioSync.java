package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class FolioSync {

    public final static String SYNCED_UNTIL_KEY = "FOLIOSyncStateSyncedUntil";

    private final static List<String> SIGEL_LIST = Collections.unmodifiableList( Arrays.asList(System.getenv("SIGEL").split(",")) );

    public static void run() throws SQLException, IOException {
        Connection connection = Storage.getConnection();
        long syncedUntil = Long.parseLong( Storage.getState(SYNCED_UNTIL_KEY, connection) );

        // Read a batch, ready for syncing to folio
        long modified = syncedUntil;
        List<Long> ids = new ArrayList<>(50);
        List<String> datas = new ArrayList<>(50);
        try (PreparedStatement statement = connection.prepareStatement("SELECT id, entity, modified FROM entities WHERE modified >= ? ORDER BY modified ASC LIMIT 50")) {
            statement.setLong(1, syncedUntil);
            statement.execute();
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getLong(1));
                    datas.add(resultSet.getString(2));
                    modified = resultSet.getLong(3);
                }
            }
        }

        // Possibly write
        for (int i = 0; i < ids.size(); ++i) {
            long id = ids.get(i);
            String data = datas.get(i);
            considerForExport(id, data, connection);
        }

        // Commit state for next pass
        if (modified > syncedUntil) {
            System.err.println("Synced folio up to: " + modified);
            Storage.writeState(SYNCED_UNTIL_KEY, "" + modified, connection);
        }
        connection.commit();
    }

    private static void considerForExport(long id, String data, Connection connection) throws SQLException, IOException {
        Map mainEntity = Storage.mapper.readValue(data, Map.class);

        // Is this a folio root record (an 'Item' held by a selected library)?
        boolean isRootRecord = false; // assumption
        if (mainEntity.get("@type") instanceof String type) {
            if (type.equals("Item")) {
                if (mainEntity.get("heldBy") instanceof Map heldBy) {
                    if (heldBy.get("@id") instanceof String heldById) {
                        String libraryCode = heldById.substring(heldById.lastIndexOf('/') + 1);
                        isRootRecord |= SIGEL_LIST.contains(libraryCode);
                    }
                }
            }
        }

        if (isRootRecord) { // If this *is* a root record, we may need to write it to folio.

            long checksum = calculateCheckSum(data);
            boolean exported = false; // assumption
            try (PreparedStatement statement = connection.prepareStatement("SELECT checksum FROM exported_checksum WHERE entity_id = ?")) {
                statement.setLong(1, id);
                statement.execute();
                try (ResultSet resultSet = statement.getResultSet()) {
                    if (resultSet.next()) {
                        long lastExportedChecksum = resultSet.getLong(1);
                        if (lastExportedChecksum != checksum) {
                            // A visible difference. Write it to folio!

                            exported = true;
                        }
                    }
                }
            }
            if (exported) {
                try (PreparedStatement statement = connection.prepareStatement("INSERT INTO exported_checksum(entity_id, checksum) VALUES(?, ?) ON CONFLICT(entity_id) DO UPDATE SET checksum=excluded.checksum")) {
                    statement.setLong(1, id);
                    statement.setLong(2, checksum);
                    statement.execute();
                }
            }

        } else { // Could it have affected another record that is a root record ?
            // TODO
        }
    }

    private static long calculateCheckSum(Object data) {
        return calculateCheckSumInternal(data, 1, 1);
    }

    private static long calculateCheckSumInternal(Object node, long depth, long offsetFactor) {
        long term = 0;

        if (node == null)
            return term;
        else if (node instanceof String s)
            return s.hashCode() * depth * offsetFactor;
        else if (node instanceof Boolean b)
            return b ? depth : term;
        else if (node instanceof Integer i)
            return i * depth * offsetFactor;
        else if (node instanceof Long l)
            return l * depth * offsetFactor;
        else if (node instanceof Map m) {
            for (Object key : m.keySet()) {
                if (!key.equals("modified")) { // FILTER OTHER IRRELEVANT "BUILDE" PROPERTIES HERE

                    term += key.hashCode() * depth;
                    term += calculateCheckSumInternal(m.get(key), depth + 1, key.hashCode());
                }
            }
        }
        else if (node instanceof List l) {
            int i = 1;
            for (Object entry : l)
                term += calculateCheckSumInternal(entry, depth + (i++), 1);
        }
        else {
            return node.hashCode() * depth;
        }

        return term;
    }
}
