package org.mariadb.jdbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;

public class LoggingTest extends BaseTest {

    @Test
    public void executionTimeLog() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Statement statement = sharedConnection.createStatement();
        statement.execute("SELECT 1");

        try (Connection connection = setConnection("&profileSQL=true")) {
            statement = connection.createStatement();
            statement.execute("SELECT 1");
            statement.executeQuery("SELECT 1");

        }
    }
}
