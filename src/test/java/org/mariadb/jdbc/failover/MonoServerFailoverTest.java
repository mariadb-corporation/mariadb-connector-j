package org.mariadb.jdbc.failover;

import org.junit.*;
import org.mariadb.jdbc.JDBCUrl;
import org.mariadb.jdbc.internal.mysql.Protocol;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public class MonoServerFailoverTest extends BaseMultiHostTest {
    private Connection connection;

    @Before
    public void init() throws SQLException {
        Assume.assumeTrue(initialUrl != null);
        currentType = TestType.NONE;
    }

    @After
    public void after() throws SQLException {
        assureProxy();
        assureBlackList(connection);
        if (connection != null) connection.close();
    }

    @Test
    public void checkClosedConnectionAfterFailover() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);

        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            st.execute("SELECT 1");
            Assert.fail();
        } catch (SQLException e) {
        }
        Assert.assertTrue(st.isClosed());
        restartProxy(masterServerId);
        try {
            st = connection.createStatement();
            st.execute("SELECT 1");
        } catch (SQLException e) {
            Assert.fail();
        }

    }

    @Test
    public void checkErrorAfterDeconnection() throws Throwable {
        connection = getNewConnection("&retriesAllDown=1", true);

        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            st.execute("SELECT 1");
            Assert.fail();
        } catch (SQLException e) {
        }

        restartProxy(masterServerId);
        try {
            st.execute("SELECT 1");
            Assert.fail();
        } catch (SQLException e) {
            //statement must be closed -> error
        }
       Assert.assertTrue(connection.isClosed());

    }


    @Test
    public void checkAutoReconnectDeconnection() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);

        Statement st = connection.createStatement();
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);
        try {
            st.execute("SELECT 1");
            Assert.fail();
        } catch (SQLException e) {
        }

        restartProxy(masterServerId);
        try {
            //with autoreconnect -> not closed
            st = connection.createStatement();
            st.execute("SELECT 1");
        } catch (SQLException e) {
            Assert.fail();
        }
        Assert.assertFalse(connection.isClosed());


    }


    /**
     * CONJ-120 Fix Connection.isValid method
     *
     * @throws Exception
     */
    @Test
    public void isValid_connectionThatIsKilledExternally() throws Throwable {
        Connection killerConnection = null;
        try {
            connection = getNewConnection();
            connection.setCatalog("mysql");
            Protocol protocol = getProtocolFromConnection(connection);
            killerConnection = getNewConnection();
            Statement killerStatement = killerConnection.createStatement();
            long threadId = protocol.getServerThreadId();
            killerStatement.execute("KILL CONNECTION " + threadId);
            killerConnection.close();
            boolean isValid = connection.isValid(0);
            assertFalse(isValid);
        } finally {
            killerConnection.close();
        }
    }

    @Test
    public void checkPrepareStatement() throws Throwable {
        connection = getNewConnection("&autoReconnect=true&retriesAllDown=1", true);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists failt1");
        stmt.execute("create table failt1 (id int not null primary key auto_increment, tt int)");


        PreparedStatement preparedStatement = connection.prepareStatement("insert into failt1(id, tt) values (?,?)");

        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);

        preparedStatement.setInt(1, 1);
        preparedStatement.setInt(2, 1);
        preparedStatement.addBatch();
        try {
            preparedStatement.executeBatch();
            Assert.fail();
        } catch (SQLException e) {

        }
        restartProxy(masterServerId);
        try {
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            Assert.fail();
        }
    }

/*
    @Test
    public void failoverDuringStreamStatement() throws Throwable {
        connection = getNewConnection("&autoReconnect=true", true);
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists failt2");
        stmt.execute("create table failt2 (tt int)");

        PreparedStatement preparedStatement = connection.prepareStatement("insert into failt2(tt) values (?)");
        for (int i=0; i<100;i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        stmt = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM failt2");
        int masterServerId = getServerId(connection);
        stopProxy(masterServerId);

        int nbRead = 0;
        try {
            while (rs.next()) {
                nbRead++;
                log.debug("nbRead = "+nbRead + " rs="+rs.getInt(1));
            }
            Assert.fail();
        } catch (SQLException e) {
            Assert.assertTrue(nbRead == 10);
        }
    }
*/
}
