package org.mariadb.jdbc.failover;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.UrlHAMode;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class CancelTest extends BaseMultiHostTest {
    private Connection connection;

    @Before
    public void init() throws SQLException {
        currentType = UrlHAMode.FAILOVER;
        initialUrl = initialGaleraUrl;
        proxyUrl = proxyGaleraUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
        connection = null;
    }

    @After
    public void after() throws SQLException {
        assureProxy();
        assureBlackList(connection);
        if (connection != null) connection.close();
    }


    @Test(expected = SQLTimeoutException.class)
    public void timeoutSleep() throws Exception {
        connection = getNewConnection(false);
        PreparedStatement stmt = connection.prepareStatement("select sleep(100)");
        stmt.setQueryTimeout(1);
        stmt.execute();
    }

    @Test
    public void NoTimeoutSleep() throws Exception {
        connection = getNewConnection(false);
        Statement stmt = connection.createStatement();
        stmt.setQueryTimeout(1);
        stmt.execute("select sleep(0.5)");

    }

    @Test
    public void CancelIdleStatement() throws Exception {
        connection = getNewConnection(false);
        Statement stmt = connection.createStatement();
        stmt.cancel();
        ResultSet rs = stmt.executeQuery("select 1");
        rs.next();
        assertEquals(rs.getInt(1), 1);
    }
}
