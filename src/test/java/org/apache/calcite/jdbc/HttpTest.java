package org.apache.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.util.Sources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class HttpTest {

    static {
        // log levels
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.INFO);
        ((Logger) LoggerFactory.getLogger("org.apache.calcite")).setLevel(Level.INFO);
    }

    HttpServer testServer;

    int port = 8080;

    @Before
    public void setUp() {

        HttpServer.Builder<?> avaticaServerBuilder = new HttpServer.Builder<>()
                .withHandler(getLocalService(), Serialization.JSON)
                .withPort(port);

        testServer = avaticaServerBuilder.build();

        testServer.start();
    }

    @After
    public void tearDown() {
        if (testServer != null) {
            testServer.stop();
        }
    }

    @Test
    public void testConcurrenClients() throws Exception {
        System.err.println("=== Test Run ===");
        IntStream.range(0, 100).parallel().forEach(idx -> {
            AvaticaTestClient testClient = new AvaticaTestClient("http://localhost:" + port);
            try (Connection conn = testClient.getConnection()){
                try (ResultSet r = conn.getMetaData().getTables(null, null, null, null)) {
                    while (r.next()) {
                        String schema = r.getString(2);
                        String tblName = r.getString(3);
                        System.out.println(Thread.currentThread().getName() + ":" + schema + "." + tblName);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        System.err.println("\n-- DONE --");
    }

    private String jsonPath(String model) {
        return Sources.of(HttpTest.class.getResource("/" + model + ".json")).file().getAbsolutePath();
    }

    private Service getLocalService() {
        Properties info = new Properties();
        info.put("model", jsonPath("model"));
        try {
            return new LocalService(new JdbcMeta("jdbc:calcite:", info));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    /**
     * Providing access to remote Avatica HTTP server using Avatica JDBC driver.
     * See https://calcite.apache.org/avatica/docs/client_reference.html
     */
    static class AvaticaTestClient {

        private String jdbcUrl;

        public AvaticaTestClient(String serverUrl) {
            // load Avatica driver
            try {
                String driverClass = org.apache.calcite.avatica.remote.Driver.class.getName();
                Class.forName(driverClass).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            jdbcUrl = "jdbc:avatica:remote:url=" + serverUrl + ";serialization=json";
        }

        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(jdbcUrl);
        }
    }
}
