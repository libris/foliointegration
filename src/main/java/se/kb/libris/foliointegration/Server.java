package se.kb.libris.foliointegration;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Server {

    private static org.eclipse.jetty.server.Server createServer() {
        int maxConnections = 50;
        var queue = new ArrayBlockingQueue<Runnable>(maxConnections);
        var pool = new ExecutorThreadPool(maxConnections, maxConnections, queue);

        var server = new org.eclipse.jetty.server.Server(pool);

        int port = 8484;

        var httpConfig = new HttpConfiguration();
        httpConfig.setIdleTimeout(5 * 60 * 1000); // more than nginx keepalive_timeout
        httpConfig.setPersistentConnectionsEnabled(true);
        httpConfig.setCustomizers(List.of(new ForwardedRequestCustomizer()));

        try (var http = new ServerConnector(server, new HttpConnectionFactory(httpConfig))) {
            http.setPort(port);
            http.setAcceptQueueSize(maxConnections);
            server.setConnectors(new Connector[]{ http });
            System.err.println("Started server on port " + port);
        }

        server.addBean(new ConnectionLimit(maxConnections, server));

        return server;
    }

    // The point of this, is to be able to request the changing of these times
    // from another thread (typically the servlet/gui thread), as other threads than
    // this main one are not allowed to write into the database. We do not want to worry
    // about concurrency with sqlite.
    private static long requestedNewEmmTime = 0;
    private static long requestedNewFolioTime = 0;
    public static synchronized void requestChangedEmmTime(long newTime) {
        requestedNewEmmTime = newTime;
    }
    public static synchronized void requestChangedFolioTime(long newTime) {
        requestedNewFolioTime = newTime;
    }
    public static synchronized long getRequestedNewEmmTime() {
        return requestedNewEmmTime;
    }
    public static synchronized long getRequestedNewFolioTime() {
        return requestedNewFolioTime;
    }

    public static void main(String[] args) throws Exception {

        var server = createServer();
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        ServletHolder holder = new ServletHolder(IntegrationServlet.class);
        holder.setInitOrder(0);
        context.addServlet(holder, "/");

        // Set the initial state if needed
        {
            Connection connection = Storage.getConnection();
            Storage.APPLICATION_STATE state = Storage.getApplicationState(connection);
            if (state == null) {
                Storage.log("No previous state detected.");
                Storage.transitionToApplicationState(Storage.APPLICATION_STATE.INITIAL_LOAD_FROM_LIBRIS, connection);
            } else {
                Storage.log("Application starting in state: " + state);
            }
            connection.commit();
        }

        server.start();

        // The main loop
        while (true) {
            try {
                Connection connection = Storage.getConnection();
                Storage.APPLICATION_STATE state = Storage.getApplicationState(connection);

                switch (state) {
                    case INITIAL_LOAD_FROM_LIBRIS:
                        EmmDumpImport.run();
                        break;
                    case STAYING_IN_SYNC: {

                        // synchronized with the requestChanged*Time-stuff above
                        synchronized (Server.class) {
                            if (requestedNewEmmTime != 0) {
                                Storage.writeState(EmmSync.SYNCED_UNTIL_KEY, "" + requestedNewEmmTime, connection);
                                Storage.log("EMM sync time MANUALLY changed to: " + requestedNewEmmTime);
                                requestedNewEmmTime = 0;
                            }
                            if (requestedNewFolioTime != 0) {
                                Storage.writeState(FolioSync.SYNCED_UNTIL_KEY, "" + requestedNewFolioTime, connection);
                                Storage.log("FOLIO sync time MANUALLY changed to: " + requestedNewFolioTime);
                                requestedNewFolioTime = 0;
                            }
                        }

                        boolean runAgainImmediately = false;
                        runAgainImmediately |= EmmSync.run();
                        runAgainImmediately |= FolioSync.run();
                        if (!runAgainImmediately) {
                            Thread.sleep(100);
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                // Outermost catch all, or we crash (on runtime exceptions)
                Storage.log("Iteration failed (will retry in 5 seconds).", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) { /* ignore */ }
            }
        }

        //server.join(); // unreachable
    }
}
