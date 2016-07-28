package org.mariadb.jdbc;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

public class LoggingTest extends BaseTest {

    @Test
    public void executionTimeLog() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream old = System.out;

        System.setOut(ps);
        try (Connection connection = setConnection("&profileSQL=true&maxQuerySizeToLog=1024")) {
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT 1");
            System.out.flush();
            String output = baos.toString();
            //output must be like
            //12:27:37.702 [main] INFO  org.mariadb.jdbc.MariaDbStatement - Query - conn:1489 - 0,75 ms - "SELECT 1"
            assertTrue(output.contains("- Query - conn:"));
            assertTrue(output.contains(" ms - \"SELECT 1\""));
        } finally {
            System.setOut(old);
        }
    }
}
