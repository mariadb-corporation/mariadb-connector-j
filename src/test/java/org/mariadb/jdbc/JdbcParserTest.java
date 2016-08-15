package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.constant.Version;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.fail;

public class JdbcParserTest {

    @Test
    public void testMariaAlias() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test");
        UrlParser jdbc2 = UrlParser.parse("jdbc:mysql://localhost/test");
        UrlParser jdbc3 = UrlParser.parse("jdbc:mariadb_" + Version.version + "://localhost/test");
        Assert.assertEquals(jdbc, jdbc2);
        Assert.assertEquals(jdbc, jdbc3);
    }

    @Test
    public void testAcceptsUrl() throws Throwable {
        Driver driver = new Driver();
        Assert.assertTrue(driver.acceptsURL("jdbc:mariadb://localhost/test"));
        Assert.assertTrue(driver.acceptsURL("jdbc:mysql://localhost/test"));
        Assert.assertTrue(driver.acceptsURL("jdbc:mariadb_" + Version.version + "://localhost/test"));
        Assert.assertFalse(driver.acceptsURL("jdbc:mariadb_1.3.6://localhost/test"));
    }

    @Test
    public void testSslAlias() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb://localhost/test?useSSL=true");
        Assert.assertTrue(jdbc.getOptions().useSsl);

        jdbc = UrlParser.parse("jdbc:mariadb://localhost/test?useSsl=true");
        Assert.assertTrue(jdbc.getOptions().useSsl);

        jdbc = UrlParser.parse("jdbc:mariadb://localhost/test");
        Assert.assertFalse(jdbc.getOptions().useSsl);
    }

    @Test
    public void testNAmePipeUrl() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb:///test?useSSL=true");
        Assert.assertTrue(jdbc.getOptions().useSsl);
    }

    @Test
    public void testOptionTakeDefault() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mysql://localhost/test");
        Assert.assertNull(jdbc.getOptions().connectTimeout);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 120);
        Assert.assertFalse(jdbc.getOptions().autoReconnect);
        Assert.assertNull(jdbc.getOptions().user);
        Assert.assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        Assert.assertNull(jdbc.getOptions().socketTimeout);

    }

    @Test
    public void testOptionTakeDefaultAurora() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mysql:aurora://cluster-identifier.cluster-customerID.region.rds.amazonaws.com/test");
        Assert.assertNull(jdbc.getOptions().connectTimeout);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 120);
        Assert.assertFalse(jdbc.getOptions().autoReconnect);
        Assert.assertNull(jdbc.getOptions().user);
        Assert.assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        Assert.assertTrue(jdbc.getOptions().socketTimeout.intValue() == 10000);
    }

    @Test
    public void testOptionParse() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mysql://localhost/test?user=root&password=toto&createDB=true"
                + "&autoReconnect=true&validConnectionTimeout=2&connectTimeout=5");
        Assert.assertTrue(jdbc.getOptions().connectTimeout == 5);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 2);
        Assert.assertTrue(jdbc.getOptions().autoReconnect);
        Assert.assertTrue(jdbc.getOptions().createDatabaseIfNotExist);

        Assert.assertTrue("root".equals(jdbc.getOptions().user));
        Assert.assertTrue("root".equals(jdbc.getUsername()));

        Assert.assertTrue("toto".equals(jdbc.getOptions().password));
        Assert.assertTrue("toto".equals(jdbc.getPassword()));
    }

    @Test
    public void testOptionParseSlash() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mysql://127.0.0.1:3306/colleo?user=root&password=toto"
                + "&localSocket=/var/run/mysqld/mysqld.sock");
        Assert.assertTrue("/var/run/mysqld/mysqld.sock".equals(jdbc.getOptions().localSocket));

        Assert.assertTrue("root".equals(jdbc.getOptions().user));
        Assert.assertTrue("root".equals(jdbc.getUsername()));

        Assert.assertTrue("toto".equals(jdbc.getOptions().password));
        Assert.assertTrue("toto".equals(jdbc.getPassword()));
    }

    @Test
    public void testOptionParseIntegerMinimum() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mysql://localhost/test?user=root&autoReconnect=true"
                + "&validConnectionTimeout=0&connectTimeout=5");
        Assert.assertTrue(jdbc.getOptions().connectTimeout == 5);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 0);
        Assert.assertTrue(jdbc.getOptions().autoReconnect);
        Assert.assertTrue("root".equals(jdbc.getOptions().user));
    }

    @Test(expected = SQLException.class)
    public void testOptionParseIntegerNotPossible() throws Throwable {
        UrlParser.parse("jdbc:mysql://localhost/test?user=root&autoReconnect=true&validConnectionTimeout=-2"
                + "&connectTimeout=5");
        fail();
    }

    @Test()
    public void testJdbcParserSimpleIpv4basic() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database";
        UrlParser.parse(url);
    }

    @Test
    public void testJdbcParserSimpleIpv4basicError() throws SQLException {
        UrlParser urlParser = UrlParser.parse(null);
        Assert.assertTrue(urlParser == null);
    }

    @Test
    public void testJdbcParserSimpleIpv4basicwithoutDatabase() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertNull(urlParser.getDatabase());
        Assert.assertNull(urlParser.getUsername());
        Assert.assertNull(urlParser.getPassword());
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserWithoutDatabaseWithProperties() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308?autoReconnect=true";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertNull(urlParser.getDatabase());
        Assert.assertNull(urlParser.getUsername());
        Assert.assertNull(urlParser.getPassword());
        Assert.assertTrue(urlParser.getOptions().autoReconnect);
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserSimpleIpv4Properties() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database?autoReconnect=true";
        Properties prop = new Properties();
        prop.setProperty("user", "greg");
        prop.setProperty("password", "pass");

        UrlParser urlParser = UrlParser.parse(url, prop);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue("greg".equals(urlParser.getUsername()));
        Assert.assertTrue("pass".equals(urlParser.getPassword()));
        Assert.assertTrue(urlParser.getOptions().autoReconnect);
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserSimpleIpv4PropertiesReversedOrder() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308?autoReconnect=true/database";
        Properties prop = new Properties();
        prop.setProperty("user", "greg");
        prop.setProperty("password", "pass");

        UrlParser urlParser = UrlParser.parse(url, prop);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue("greg".equals(urlParser.getUsername()));
        Assert.assertTrue("pass".equals(urlParser.getPassword()));
        Assert.assertTrue(urlParser.getOptions().autoReconnect);
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserSimpleIpv4() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue("greg".equals(urlParser.getUsername()));
        Assert.assertTrue("pass".equals(urlParser.getPassword()));
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(urlParser.getHostAddresses().get(2)));
    }


    @Test
    public void testJdbcParserSimpleIpv6() throws SQLException {
        String url = "jdbc:mysql://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306,[2001:660:7401:200::edf:bdd7]:3307"
                + "/database?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue("greg".equals(urlParser.getUsername()));
        Assert.assertTrue("pass".equals(urlParser.getPassword()));
        Assert.assertTrue(urlParser.getHostAddresses().size() == 2);
        Assert.assertTrue(new HostAddress("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306)
                .equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("2001:660:7401:200::edf:bdd7", 3307)
                .equals(urlParser.getHostAddresses().get(1)));
    }


    @Test
    public void testJdbcParserParameter() throws SQLException {
        String url = "jdbc:mysql://address=(type=master)(port=3306)(host=master1),address=(port=3307)(type=master)"
                + "(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue("greg".equals(urlParser.getUsername()));
        Assert.assertTrue("pass".equals(urlParser.getPassword()));
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("master2", 3307, "master").equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave1", 3308, "slave").equals(urlParser.getHostAddresses().get(2)));
    }

    @Test()
    public void testJdbcParserParameterErrorEqual() {
        String url = "jdbc:mysql://address=(type=)(port=3306)(host=master1),address=(port=3307)(type=master)"
                + "(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        try {
            UrlParser.parse(url);
            fail();
        } catch (SQLException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testJdbcParserHaModeNone() throws SQLException {
        String url = "jdbc:mysql://localhost/database";
        UrlParser jdbc = UrlParser.parse(url);
        Assert.assertTrue(jdbc.getHaMode().equals(HaMode.NONE));
    }

    @Test
    public void testJdbcParserHaModeLoadReplication() throws SQLException {
        String url = "jdbc:mysql:replication://localhost/database";
        UrlParser jdbc = UrlParser.parse(url);
        Assert.assertTrue(jdbc.getHaMode().equals(HaMode.REPLICATION));
    }

    @Test
    public void testJdbcParserReplicationParameter() throws SQLException {
        String url = "jdbc:mysql:replication://address=(type=master)(port=3306)(host=master1),address=(port=3307)"
                + "(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database"
                + "?user=greg&password=pass";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue("greg".equals(urlParser.getUsername()));
        Assert.assertTrue("pass".equals(urlParser.getPassword()));
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("master2", 3307, "master").equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave1", 3308, "slave").equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserReplicationParameterWithoutType() throws SQLException {
        String url = "jdbc:mysql:replication://master1,slave1,slave2/database";
        UrlParser urlParser = UrlParser.parse(url);
        Assert.assertTrue("database".equals(urlParser.getDatabase()));
        Assert.assertTrue(urlParser.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(urlParser.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3306, "slave").equals(urlParser.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3306, "slave").equals(urlParser.getHostAddresses().get(2)));
    }

    @Test
    public void testJdbcParserHaModeLoadAurora() throws SQLException {
        String url = "jdbc:mysql:aurora://cluster-identifier.cluster-customerID.region.rds.amazonaws.com/database";
        UrlParser jdbc = UrlParser.parse(url);
        Assert.assertTrue(jdbc.getHaMode().equals(HaMode.AURORA));
    }

    /**
     * Conj-167 : Driver is throwing IllegalArgumentException instead of returning null.
     */
    @Test
    public void checkOtherDriverCompatibility() throws SQLException {
        String url = "jdbc:h2:mem:RZM;DB_CLOSE_DELAY=-1";
        UrlParser jdbc = UrlParser.parse(url);
        Assert.assertTrue(jdbc == null);
    }

}
