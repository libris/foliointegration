package se.kb.libris.foliointegration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.ZonedDateTime;

import org.sqlite.SQLiteConfig;

public class Storage {

    public enum APPLICATION_STATE {
        // We're getting a dump out of Libris
        INITIAL_LOAD_FROM_LIBRIS,

        // We're finished downloading a dump from libris, and we're now writing that dump into
        // folio. This does *not* happen in parallel with the above, because the dump download
        // can fail, or be restarted before finishing, which would (if these ran in parallel)
        // mean a broken state in folio requiring a manual clear of all folio data before being
        // able to proceed.
        INITIAL_LOAD_TO_FOLIO,

        // Libris and folio are now up to date with respect to the initial dump. In this state
        // we keep the two in sync (or catch up to a synced state if we've fallen behind) using
        // iterative updates.
        STAYING_IN_SYNC,
    }

    private static final String APPLICATION_STATE_KEY = "ApplicationState";

    private static Connection _connection = null;

    public static void log(String message) {
        synchronized (System.err) {
            System.err.println(ZonedDateTime.now() + ": " + message);
        }
    }

    public static void logWithCallstack(String message) {
        synchronized (System.err) {
            System.err.println(ZonedDateTime.now() + ": " + message + " at:");
            StackTraceElement[] frames = Thread.currentThread().getStackTrace();
            for (int i = 2; i < frames.length; ++i) {
                System.err.println(frames[i]);
            }
            System.err.println("---------------");
        }
    }

    public static void log(String message, Exception e) {
        synchronized (System.err) {
            System.err.println(ZonedDateTime.now() + ": " + message);
            e.printStackTrace(System.err);
            System.err.println("---------------");
        }
    }

    public static synchronized void transitionToApplicationState(APPLICATION_STATE newState) {
        // The possible/allowed state transitions for this application are:
        // [null] -> "initial loading from libris"
        // "initial loading from libris" -> "initial loading of folio"
        // "initial loading of folio" -> "staying in sync"

        APPLICATION_STATE currentState = getApplicationState();

        boolean transitionIsOk = false;
        if (currentState == null && newState == APPLICATION_STATE.INITIAL_LOAD_FROM_LIBRIS) {
            transitionIsOk = true;
        } else if (currentState == APPLICATION_STATE.INITIAL_LOAD_FROM_LIBRIS && newState == APPLICATION_STATE.INITIAL_LOAD_TO_FOLIO) {
            transitionIsOk = true;
        } else if (currentState == APPLICATION_STATE.INITIAL_LOAD_TO_FOLIO && newState == APPLICATION_STATE.STAYING_IN_SYNC) {
            transitionIsOk = true;
        } else {
            logWithCallstack("ERROR: CRITICAL! State transition from " + currentState + " to " + newState + " attempted (refused). This is a BUG!");
        }

        if (transitionIsOk) {
            Storage.writeState(APPLICATION_STATE_KEY, newState.toString());
            String currentDescription = currentState == null ? "[uninitialized]" : currentState.toString();
            log("Transitioned from state: " + currentDescription + " to " + newState);
        }
    }

    public static APPLICATION_STATE getApplicationState() {
        String stateString = getState(APPLICATION_STATE_KEY);
        if (stateString == null)
            return null;

        for (APPLICATION_STATE state : APPLICATION_STATE.values()) {
            if (state.toString().equals(stateString))
                return state;
        }

        logWithCallstack("ERROR: Corrupt state: " + stateString);
        System.exit(1);
        return null;
    }

    private static synchronized void initDb(Connection connection) throws SQLException {
        {
            String sql = """
                    CREATE TABLE entities (
                        id INTEGER PRIMARY KEY,
                        entity TEXT
                    );
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE TABLE uris (
                          id INTEGER PRIMARY KEY,
                          uri TEXT,
                          entity_id INTEGER,
                          UNIQUE(uri, entity_id) ON CONFLICT IGNORE,
                          FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
                      );
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE TABLE state (
                            id INTEGER PRIMARY KEY,
                            key TEXT UNIQUE,
                            value TEXT
                        );
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE INDEX idx_uris_uri ON uris(uri);
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE INDEX idx_uris_entity_id ON uris(entity_id);
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }

    private static synchronized Connection getConnection() {
        if (_connection != null)
            return _connection;

        String dbPath = "/data/libris.sqlite3"; // default, and typically in use when running in container
        if (System.getProperty("DBPATH") != null)
            dbPath = System.getProperty("DBPATH"); // For native running, allow this to be set (typically: -DDBPATH=/tmp)

        boolean preExistingState = Files.exists(Path.of(dbPath));

        var url = "jdbc:sqlite:" + dbPath;
        try {
            SQLiteConfig config = new SQLiteConfig();
            config.setJournalMode(SQLiteConfig.JournalMode.WAL);
            config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
            config.setPragma(SQLiteConfig.Pragma.FOREIGN_KEYS, "ON");
            _connection = DriverManager.getConnection(url);

            if (!preExistingState) {
                initDb(_connection);
            }

            return _connection;
        } catch (Exception e) {
            log("Did you forget to mount the /data volume? SQLITE3 failure (unrecoverable).", e);
            System.exit(1);
        }
        return null; // can't happen
    }

    public static synchronized void writeState(String key, String value) {
        Connection connection = getConnection();
        String sql = """
                    INSERT INTO STATE (key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value;
                    """.stripIndent();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            statement.setString(2, value);
            statement.execute();
        } catch (SQLException e) {
            log("Could not write state (unrecoverable).", e);
            System.exit(1);
        }
    }

    public static synchronized void clearState(String key) {
        Connection connection = getConnection();
        String sql = """
                    DELETE FROM STATE WHERE KEY = ?;
                    """.stripIndent();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            statement.execute();
        } catch (SQLException e) {
            log("Could not clear state (unrecoverable).", e);
            System.exit(1);
        }
    }

    public static synchronized String getState(String key) {
        Connection connection = getConnection();
        String sql = """
                    SELECT value FROM STATE WHERE key = ?;
                    """.stripIndent();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            statement.execute();
            try(ResultSet resultSet = statement.getResultSet()) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            log("Could not read state (unrecoverable).", e);
            System.exit(1);
        }
        return null;
    }
}
