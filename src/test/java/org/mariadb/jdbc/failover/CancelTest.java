package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class CancelTest extends BaseMultiHostTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() throws SQLException {
        proxyUrl = proxyGaleraUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
    }

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @Before
    public void init() throws SQLException {
        defaultUrl = initialGaleraUrl;
        currentType = HaMode.FAILOVER;
    }


    @Test(expected = SQLTimeoutException.class)
    public void timeoutSleep() throws Exception {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            PreparedStatement stmt = connection.prepareStatement("select sleep(100)");
            stmt.setQueryTimeout(1);
            stmt.execute();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void noTimeoutSleep() throws Exception {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.setQueryTimeout(1);
            stmt.execute("select sleep(0.5)");
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }

    @Test
    public void cancelIdleStatement() throws Exception {
        Connection connection = null;
        try {
            connection = getNewConnection(false);
            Statement stmt = connection.createStatement();
            stmt.cancel();
            ResultSet rs = stmt.executeQuery("select 1");
            rs.next();
            assertEquals(rs.getInt(1), 1);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
