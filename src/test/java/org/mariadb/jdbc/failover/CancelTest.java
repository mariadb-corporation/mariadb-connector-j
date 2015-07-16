package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.BaseTest;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class CancelTest extends  BaseMultiHostTest {
    private Connection connection;

    @Before
    public void init() throws SQLException {
        currentType = BaseMultiHostTest.TestType.GALERA;
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



    @Test (expected = SQLTimeoutException.class)
    public void timeoutSleep() throws Exception{
        connection = getNewConnection(false);
           PreparedStatement stmt = connection.prepareStatement("select sleep(100)");
           stmt.setQueryTimeout(1);
           stmt.execute();
     }

    @Test
    public void NoTimeoutSleep() throws Exception{
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
        assertEquals(rs.getInt(1),1);
    }
}
