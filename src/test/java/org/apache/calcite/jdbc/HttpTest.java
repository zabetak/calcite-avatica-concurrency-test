package org.apache.calcite.jdbc;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.IntStream;

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

        IntStream.range(0, 20).parallel().forEach(idx -> {
            try {
                AvaticaTestClient testClient = new AvaticaTestClient("http://localhost:" + port);
                Connection conn = testClient.getConnection();

                conn.getMetaData().getTables(null, null, null, null);
                for (String table : Arrays.asList("DEPTS", "EMPS", "SDEPTS")) {
                    conn.createStatement().executeQuery("select * from " + table);
                }

                conn.close();

                System.err.print("*");
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

        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            System.err.println("=== Server Setup ===");
            output(connection.getMetaData().getTables(connection.getCatalog(), null, null, null));
            return new LocalService(new CalciteMetaImpl((CalciteConnectionImpl) connection));
        } catch (Exception e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e1) {
                    throw new RuntimeException(e1);
                }
            }
            throw new RuntimeException(e);
        }
    }

    private void output(ResultSet resultSet) throws SQLException {
        PrintStream out = System.out;

        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1;; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
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

            Serialization serialization = Serialization.JSON;

            jdbcUrl = "jdbc:avatica:remote:url=" + serverUrl
                    + ";serialization=" + serialization.name().toLowerCase();
        }

        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(jdbcUrl);
        }
    }
}
