package se.kb.libris.foliointegration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class LowPrioritySync {
    public static boolean run() throws SQLException, IOException, InterruptedException {
        Connection connection = Storage.getConnection();

        try {

            List<Long> ids = new ArrayList<>(20);
            try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM low_prio_export_queue LIMIT 20")) {
                statement.execute();
                try (ResultSet resultSet = statement.getResultSet()) {
                    while (resultSet.next()) {
                        ids.add(resultSet.getLong(1));
                    }
                }
            }

            HashSet<String> writtenHRIDs = new HashSet<>(20);

            for (Long id : ids) {

                // IDs that show up here are strictly sqlite3 rowids to HOLDING records that are to be exported.

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
                if (data == null) {
                    Storage.log("Lowprio Q held deleted record? rowid " + id);
                    continue;
                }
                Map mainEntity = Storage.mapper.readValue(data, Map.class);

                Records.embellishWithLocalData(mainEntity, new HashSet<>(), connection);
                mainEntity = Format.formatForFolio(mainEntity, connection);
                long checksum = FolioSync.calculateCheckSum(mainEntity);

                // Note that this HRID is for the instance, and not the selected item.
                String hrid = (String) ((Map) mainEntity.get("instance")).get("hrid");

                //Storage.log("Considering a LOW PRIO WRITE of rowid: " + id + " an item of " + hrid);

                boolean export = true; // assumption
                try (PreparedStatement statement = connection.prepareStatement("SELECT checksum FROM exported_checksum WHERE entity_id = ?")) {
                    statement.setLong(1, id);
                    statement.execute();
                    try (ResultSet resultSet = statement.getResultSet()) {
                        if (resultSet.next()) {
                            long lastExportedChecksum = resultSet.getLong(1);
                            //Storage.log("  ..checksum comparison: " + checksum + " == " + lastExportedChecksum);
                            if (lastExportedChecksum == checksum) {
                                export = false;
                            }
                        }
                    }
                }
                if (export) {
                    FolioWriting.queueForExport(mainEntity, connection);
                    writtenHRIDs.add(hrid);
                    try (PreparedStatement statement = connection.prepareStatement("INSERT INTO exported_checksum(entity_id, hrid, checksum) VALUES(?, ?, ?) ON CONFLICT(entity_id) DO UPDATE SET checksum=excluded.checksum")) {
                        statement.setLong(1, id);
                        statement.setString(2, hrid);
                        statement.setLong(3, checksum);
                        statement.execute();
                    }
                }
                try (PreparedStatement statement = connection.prepareStatement("DELETE FROM low_prio_export_queue WHERE id = ?")) {
                    statement.setLong(1, id);
                    statement.execute();
                }

            }

            if (FolioWriting.finalizePendingWrites(connection)) {
                if (!writtenHRIDs.isEmpty())
                    Storage.log("Low priority batch written: " + writtenHRIDs);
                return true;
            }

        } catch (Exception e) {
            Storage.log("Failure in low-priority writing queue.", e);
            connection.rollback();
        }
        return false;
    }
}
