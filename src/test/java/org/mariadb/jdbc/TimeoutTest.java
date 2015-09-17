package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeoutTest extends BaseTest {

    private static int selectValue(Connection conn, int value)
            throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select " + value);
            rs.next();
            return rs.getInt(1);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    /**
     * CONJ-79
     *
     * @throws SQLException
     */
    @Test
    public void resultSetAfterSocketTimeoutTest() throws SQLException {
        setConnection("&connectTimeout=5&socketTimeout=5");
        boolean bugReproduced = false;
        int exc = 0;
        int went = 0;
        for (int i = 0; i < 10000; i++) {
            try {
                int v1 = selectValue(connection, 1);
                int v2 = selectValue(connection, 2);
                if (v1 != 1 || v2 != 2) {
                    bugReproduced = true;
                    break;
                }
                assertTrue(v1 == 1 && v2 == 2);
                went++;
            } catch (Exception e) {
                exc++;
            }
        }
        System.out.println("exception="+exc);
        System.out.println("well="+went);
        assertFalse(bugReproduced); // either Exception or fine
        assertTrue(went > 0);
        assertTrue(went + exc == 10000);
    }

    /**
     * CONJ-79
     *
     * @throws SQLException
     */
    @Test
    public void socketTimeoutTest() throws SQLException {
        int exceptionCount = 0;
        // set a short connection timeout
        setConnection("&connectTimeout=500&socketTimeout=500");
        PreparedStatement ps = connection.prepareStatement("SELECT 1");
        ResultSet rs = ps.executeQuery();
        rs.next();
        logInfo(rs.getString(1));

        // wait for the connection to time out
        ps = connection.prepareStatement("SELECT sleep(1)");

        // a timeout should occur here
        try {
            rs = ps.executeQuery();
            Assert.fail();
        } catch (SQLException e) {
            // check that it's a timeout that occurs
            if (e.getMessage().contains("timed out"))
                exceptionCount++;
        }

        try {
            ps = connection.prepareStatement("SELECT 2");
            ps.execute();
            Assert.fail();
        } catch (Exception e) {

        }

        // the connection should  be closed
        assertTrue(connection.isClosed());
    }

    @Test
    public void waitTimeoutStatementTest() throws SQLException, InterruptedException {
        Statement statement = connection.createStatement();
        statement.execute("set session wait_timeout=1");
        Thread.sleep(2000); // Wait for the server to kill the connection

        logInfo(connection.toString());

        // here a SQLNonTransientConnectionException is expected
        // "Could not read resultset: unexpected end of stream, ..."
        try {
            statement.execute("SELECT 1");
            Assert.fail();
        } catch (SQLException e) {

        }

        statement.close();
        connection.close();
        connection = null;
    }

    @Test
    public void waitTimeoutResultSetTest() throws SQLException, InterruptedException {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");

        rs.next();

        stmt.execute("set session wait_timeout=1");
        Thread.sleep(3000); // Wait for the server to kill the connection

        // here a SQLNonTransientConnectionException is expected
        // "Could not read resultset: unexpected end of stream, ..."
        try {
            rs = stmt.executeQuery("SELECT 2");
            rs.next();
        } catch (SQLException e) {
        }
    }

    // CONJ-68
    // TODO: this test is not yet able to repeat the bug. Ignore until then.
    @Ignore
    @Test
    public void lastPacketFailedTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("DROP TABLE IF EXISTS `pages_txt`");
        stmt.execute("CREATE TABLE `pages_txt` (`id` INT(10) UNSIGNED NOT NULL, `title` TEXT NOT NULL, `txt` MEDIUMTEXT NOT NULL, PRIMARY KEY (`id`)) COLLATE='utf8_general_ci' ENGINE=MyISAM;");

        //create arbitrary long strings
        String chars = "0123456789abcdefghijklmnopqrstuvwxyzåäöABCDEFGHIJKLMNOPQRSTUVWXYZÅÄÖ,;.:-_*¨^+?!<>#€%&/()=";
        StringBuffer outputBuffer = null;
        Random r = null;

        for (int i = 1; i < 2001; i++) {
            r = new Random();
            outputBuffer = new StringBuffer(i);

            for (int j = 0; j < i; j++) {
                outputBuffer.append(chars.charAt(r.nextInt(chars.length())));
            }
            stmt.execute("insert into pages_txt values (" + i + ", '" + outputBuffer.toString() + "' , 'txt')");
        }
    }
}