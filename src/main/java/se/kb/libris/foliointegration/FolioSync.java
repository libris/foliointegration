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
        try {
            long syncedUntil = Long.parseLong(Storage.getState(SYNCED_UNTIL_KEY, connection));

            // Read a batch, ready for syncing to folio
            long modified = syncedUntil;
            List<Long> ids = new ArrayList<>(50);
            try (PreparedStatement statement = connection.prepareStatement("SELECT id, modified FROM entities WHERE modified > ? ORDER BY modified ASC LIMIT 50")) {
                statement.setLong(1, syncedUntil);
                statement.execute();
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) {
                        ids.add(resultSet.getLong(1));
                        modified = resultSet.getLong(2);
                    }
                }
            }

            // Possibly write
            for (int i = 0; i < ids.size(); ++i) {
                long id = ids.get(i);
                var cycleProtection = new HashSet<Long>();
                considerForExport(id, cycleProtection, connection);
            }

            // Commit state for next pass
            FolioWriting.flushQueue();
            if (modified > syncedUntil) {
                Storage.writeState(SYNCED_UNTIL_KEY, "" + modified, connection);
            }
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            Storage.log("ERROR: Failed to write complete update batch to FOLIO.", e);
        }

    }

    private static void considerForExport(long id, Set<Long> cycleProtection, Connection connection) throws SQLException, IOException {
        if (cycleProtection.contains(id))
            return;
        cycleProtection.add(id);

        // Read the record data
        String data = null;
        try (PreparedStatement statement = connection.prepareStatement("SELECT entity FROM entities WHERE id = ?")) {
            statement.setLong(1, id);
            statement.execute();
            try (ResultSet resultSet = statement.getResultSet()) {
                if (resultSet.next()) {
                    data = resultSet.getString(1);
                }
            }
        }
        if (data == null)
            return;
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

        // If this *is* a root record, we may need to write it to folio.
        if (isRootRecord) {

            // Has this record already been exported with this checksum (then we should skip it).
            Records.embellishWithLocalData(mainEntity, new HashSet<>(), connection);
            long checksum = calculateCheckSum(mainEntity);
            boolean export = true; // assumption
            try (PreparedStatement statement = connection.prepareStatement("SELECT checksum FROM exported_checksum WHERE entity_id = ?")) {
                statement.setLong(1, id);
                statement.execute();
                try (ResultSet resultSet = statement.getResultSet()) {
                    if (resultSet.next()) {
                        long lastExportedChecksum = resultSet.getLong(1);
                        if (lastExportedChecksum == checksum) {
                            export = false;
                        }
                    }
                }
            }
            if (export) {
                // A visible difference. Write it to folio!
                //System.err.println(" ** WRITE OF: " + mainEntity);
                FolioWriting.queueForExport(mainEntity);
                try (PreparedStatement statement = connection.prepareStatement("INSERT INTO exported_checksum(entity_id, checksum) VALUES(?, ?) ON CONFLICT(entity_id) DO UPDATE SET checksum=excluded.checksum")) {
                    statement.setLong(1, id);
                    statement.setLong(2, checksum);
                    statement.execute();
                }
            }

        } else { // If not (a root record): Could it have affected another record that is a root record ?

            // THIS ONE COULD GET HUGE.
            // A Long (upper-case L) object uses 24 bytes.
            // A long (lower-case l), primitive, uses only 8 but can't be stored in a list.
            // Consider doing something better here, perhaps.
            List<Long> possiblyAffectedIDs = new ArrayList<>();

            if (mainEntity.get("@id") instanceof String mainEntityId) {

                try (PreparedStatement statement = connection.prepareStatement("SELECT entity_id FROM referenced_uris WHERE referenced_uri = ?")) {
                    statement.setString(1, mainEntityId);
                    statement.execute();
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            long referencingEntityId = resultSet.getLong(1);
                            possiblyAffectedIDs.add( referencingEntityId );
                        }
                    }
                }

            }

            for (Long referencingEntityId : possiblyAffectedIDs) {
                considerForExport(referencingEntityId, cycleProtection, connection);
            }

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
