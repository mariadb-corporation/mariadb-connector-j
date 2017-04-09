package org.mariadb.jdbc;

import org.junit.Test;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class JdbcParserTest {

    @Test
    public void testMariaAlias() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test");
        UrlParser jdbc2 = UrlParser.parse("jdbc:mysql://localhost/test");
        assertEquals(jdbc, jdbc2);
    }

    @Test
    public void testAuroraUseBatchMultiSend() throws Throwable {
        UrlParser notAuroraDatas = UrlParser.parse("jdbc:mariadb://localhost/test?useBatchMultiSend=true");
        assertTrue(notAuroraDatas.getOptions().useBatchMultiSend);

        UrlParser auroraDatas = UrlParser.parse("jdbc:mariadb:aurora://localhost/test?useBatchMultiSend=true");
        assertFalse(auroraDatas.getOptions().useBatchMultiSend);
    }


    @Test
    public void testAcceptsUrl() throws Throwable {
        Driver driver = new Driver();
        assertTrue(driver.acceptsURL("jdbc:mariadb://localhost/test"));
        assertTrue(driver.acceptsURL("jdbc:mysql://localhost/test"));
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost/test?disableMariaDbDriver"));
    }

    @Test
    public void testSslAlias() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test?useSSL=true");
        assertTrue(jdbc.getOptions().useSsl);

        jdbc = UrlParser.parse("jdbc:mariadb://localhost/test?useSsl=true");
        assertTrue(jdbc.getOptions().useSsl);

        jdbc = UrlParser.parse("jdbc:mariadb://localhost/test");
        assertFalse(jdbc.getOptions().useSsl);
    }

    @Test
    public void testNAmePipeUrl() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb:///test?useSSL=true");
        assertTrue(jdbc.getOptions().useSsl);
    }

    @Test
    public void testOptionTakeDefault() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test");
        assertNull(jdbc.getOptions().connectTimeout);
        assertTrue(jdbc.getOptions().validConnectionTimeout == 120);
        assertFalse(jdbc.getOptions().autoReconnect);
        assertNull(jdbc.getOptions().user);
        assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        assertNull(jdbc.getOptions().socketTimeout);

    }

    @Test
    public void testOptionTakeDefaultAurora() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb:aurora://cluster-identifier.cluster-customerID.region.rds.amazonaws.com/test");
        assertNull(jdbc.getOptions().connectTimeout);
        assertTrue(jdbc.getOptions().validConnectionTimeout == 120);
        assertFalse(jdbc.getOptions().autoReconnect);
        assertNull(jdbc.getOptions().user);
        assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        assertTrue(jdbc.getOptions().socketTimeout.intValue() == 10000);
    }

    @Test
    public void testOptionParse() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test?user=root&password=toto&createDB=true"
                + "&autoReconnect=true&validConnectionTimeout=2&connectTimeout=5");
        assertTrue(jdbc.getOptions().connectTimeout == 5);
        assertTrue(jdbc.getOptions().validConnectionTimeout == 2);
        assertTrue(jdbc.getOptions().autoReconnect);
        assertTrue(jdbc.getOptions().createDatabaseIfNotExist);

        assertTrue("root".equals(jdbc.getOptions().user));
        assertTrue("root".equals(jdbc.getUsername()));

        assertTrue("toto".equals(jdbc.getOptions().password));
        assertTrue("toto".equals(jdbc.getPassword()));
    }

    @Test
    public void testOptionParseSlash() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://127.0.0.1:3306/colleo?user=root&password=toto"
                + "&localSocket=/var/run/mysqld/mysqld.sock");
        assertTrue("/var/run/mysqld/mysqld.sock".equals(jdbc.getOptions().localSocket));

        assertTrue("root".equals(jdbc.getOptions().user));
        assertTrue("root".equals(jdbc.getUsername()));

        assertTrue("toto".equals(jdbc.getOptions().password));
        assertTrue("toto".equals(jdbc.getPassword()));
    }

    @Test
    public void testOptionParseIntegerMinimum() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test?user=root&autoReconnect=true"
                + "&validConnectionTimeout=0&connectTimeout=5");
        assertTrue(jdbc.getOptions().connectTimeout == 5);
        assertTrue(jdbc.getOptions().validConnectionTimeout == 0);
        assertTrue(jdbc.getOptions().autoReconnect);
        assertTrue("root".equals(jdbc.getOptions().user));
    }

    @Test(expected = SQLException.class)
    public void testOptionParseIntegerNotPossible() throws Throwable {
        UrlParser.parse("jdbc:mariadb://localhost/test?user=root&autoReconnect=true&validConnectionTimeout=-2"
                + "&connectTimeout=5");
        fail();
    }

    @Test()
    public void testJdbcParserSimpleIpv4basic() throws SQLException {
        String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database";
        UrlParser.parse(url);
    }

    @Test
    public void testJdbcParserSimpleIpv4basicError() throws SQLException {
        UrlParser urlParser = UrlParser.parse(null);
        assertTrue(urlParser == null);
    }

    @Test
    public void testJdbcParserSimpleIpv4basicwithoutDatabase() throws SQLException {
        String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/";
        UrlParser urlParser = UrlParser.parse(url);
        assertNull(urlParser.getDatabase());
        assertNull(urlParser.getUsername());
        assertNull(urlParser.getPassword());
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserWithoutDatabaseWithProperties() throws SQLException {
        String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308?autoReconnect=true";
        UrlParser urlParser = UrlParser.parse(url);
        assertNull(urlParser.getDatabase());
        assertNull(urlParser.getUsername());
        assertNull(urlParser.getPassword());
        assertTrue(urlParser.getOptions().autoReconnect);
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserSimpleIpv4Properties() throws SQLException {
        String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database?autoReconnect=true";
        Properties prop = new Properties();
        prop.setProperty("user", "greg");
        prop.setProperty("password", "pass");

        UrlParser urlParser = UrlParser.parse(url, prop);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue("greg".equals(urlParser.getUsername()));
        assertTrue("pass".equals(urlParser.getPassword()));
        assertTrue(urlParser.getOptions().autoReconnect);
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserSimpleIpv4PropertiesReversedOrder() throws SQLException {
        String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308?autoReconnect=true/database";
        Properties prop = new Properties();
        prop.setProperty("user", "greg");
        prop.setProperty("password", "pass");

        UrlParser urlParser = UrlParser.parse(url, prop);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue("greg".equals(urlParser.getUsername()));
        assertTrue("pass".equals(urlParser.getPassword()));
        assertTrue(urlParser.getOptions().autoReconnect);
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserSimpleIpv4() throws SQLException {
        String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue("greg".equals(urlParser.getUsername()));
        assertTrue("pass".equals(urlParser.getPassword()));
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }


    @Test
    public void testJdbcParserSimpleIpv6() throws SQLException {
        String url = "jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306,[2001:660:7401:200::edf:bdd7]:3307"
                + "/database?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue("greg".equals(urlParser.getUsername()));
        assertTrue("pass".equals(urlParser.getPassword()));
        assertTrue(urlParser.getHostAddresses().size() == 2);
        assertTrue(new HostAddress("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306)
                .equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("2001:660:7401:200::edf:bdd7", 3307)
                .equals(urlParser.getHostAddresses().get(1)));
    }


    @Test
    public void testJdbcParserParameter() throws SQLException {
        String url = "jdbc:mariadb://address=(type=master)(port=3306)(host=master1),address=(port=3307)(type=master)"
                + "(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue("greg".equals(urlParser.getUsername()));
        assertTrue("pass".equals(urlParser.getPassword()));
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master1", 3306, "master").equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("master2", 3307, "master").equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave1", 3308, "slave").equals(urlParser.getHostAddresses().get(2)));
    }

    @Test(expected = SQLException.class)
    public void testJdbcParserParameterErrorEqual() throws SQLException {
        String url = "jdbc:mariadb://address=(type=)(port=3306)(host=master1),address=(port=3307)(type=master)"
                + "(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        UrlParser.parse(url);
        fail("Must have throw an SQLException");
    }

    @Test
    public void testJdbcParserHaModeNone() throws SQLException {
        String url = "jdbc:mariadb://localhost/database";
        UrlParser jdbc = UrlParser.parse(url);
        assertTrue(jdbc.getHaMode().equals(HaMode.NONE));
    }

    @Test
    public void testJdbcParserHaModeLoadReplication() throws SQLException {
        String url = "jdbc:mariadb:replication://localhost/database";
        UrlParser jdbc = UrlParser.parse(url);
        assertTrue(jdbc.getHaMode().equals(HaMode.REPLICATION));
    }

    @Test
    public void testJdbcParserReplicationParameter() throws SQLException {
        String url = "jdbc:mariadb:replication://address=(type=master)(port=3306)(host=master1),address=(port=3307)"
                + "(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database"
                + "?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue("greg".equals(urlParser.getUsername()));
        assertTrue("pass".equals(urlParser.getPassword()));
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master1", 3306, "master").equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("master2", 3307, "master").equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave1", 3308, "slave").equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserReplicationParameterWithoutType() throws SQLException {
        String url = "jdbc:mariadb:replication://master1,slave1,slave2/database";
        UrlParser urlParser = UrlParser.parse(url);
        assertTrue("database".equals(urlParser.getDatabase()));
        assertTrue(urlParser.getHostAddresses().size() == 3);
        assertTrue(new HostAddress("master1", 3306, "master").equals(urlParser.getHostAddresses().get(0)));
        assertTrue(new HostAddress("slave1", 3306, "slave").equals(urlParser.getHostAddresses().get(1)));
        assertTrue(new HostAddress("slave2", 3306, "slave").equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserHaModeLoadAurora() throws SQLException {
        String url = "jdbc:mariadb:aurora://cluster-identifier.cluster-customerID.region.rds.amazonaws.com/database";
        UrlParser jdbc = UrlParser.parse(url);
        assertTrue(jdbc.getHaMode().equals(HaMode.AURORA));
    }

    /**
     * Conj-167 : Driver is throwing IllegalArgumentException instead of returning null.
     * @throws SQLException if any exception occur
     */
    @Test
    public void checkOtherDriverCompatibility() throws SQLException {
        UrlParser jdbc = UrlParser.parse("jdbc:h2:mem:RZM;DB_CLOSE_DELAY=-1");
        assertTrue(jdbc == null);
    }

    /**
     * CONJ-423] driver doesn't accept connection string with "disableMariaDbDriver".
     * @throws SQLException if any exception occur
     */
    @Test
    public void checkDisable() throws SQLException {
        UrlParser jdbc = UrlParser.parse("jdbc:mysql://localhost/test?disableMariaDbDriver");
        assertTrue(jdbc == null);
    }


}