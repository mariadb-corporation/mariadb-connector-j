package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.internal.common.UrlHAMode;

import java.sql.SQLException;
import java.util.Properties;

public class JdbcParserTest {

    @Test
    public void testOptionTakeDefault() throws Throwable {
        JDBCUrl jdbc = JDBCUrl.parse("jdbc:mysql://localhost/test");
        Assert.assertNull(jdbc.getOptions().connectTimeout);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 120);
        Assert.assertFalse(jdbc.getOptions().autoReconnect);
        Assert.assertNull(jdbc.getOptions().user);
        Assert.assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        Assert.assertNull(jdbc.getOptions().socketTimeout);

    }

    @Test
    public void testOptionTakeDefaultAurora() throws Throwable {
        JDBCUrl jdbc = JDBCUrl.parse("jdbc:mysql:aurora://localhost/test");
        Assert.assertNull(jdbc.getOptions().connectTimeout);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 120);
        Assert.assertFalse(jdbc.getOptions().autoReconnect);
        Assert.assertNull(jdbc.getOptions().user);
        Assert.assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        Assert.assertTrue(jdbc.getOptions().socketTimeout.intValue() == 10000);
    }

    @Test
    public void testOptionParse() throws Throwable {
        JDBCUrl jdbc = JDBCUrl.parse("jdbc:mysql://localhost/test?user=root&password=toto&createDB=true&autoReconnect=true&validConnectionTimeout=2&connectTimeout=5");
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
        JDBCUrl jdbc = JDBCUrl.parse("jdbc:mysql://127.0.0.1:3306/colleo?user=root&password=toto&localSocket=/var/run/mysqld/mysqld.sock");
        Assert.assertTrue("/var/run/mysqld/mysqld.sock".equals(jdbc.getOptions().localSocket));

        Assert.assertTrue("root".equals(jdbc.getOptions().user));
        Assert.assertTrue("root".equals(jdbc.getUsername()));

        Assert.assertTrue("toto".equals(jdbc.getOptions().password));
        Assert.assertTrue("toto".equals(jdbc.getPassword()));
    }
    @Test
    public void testOptionParseIntegerMinimum() throws Throwable {
        JDBCUrl jdbc = JDBCUrl.parse("jdbc:mysql://localhost/test?user=root&autoReconnect=true&validConnectionTimeout=0&connectTimeout=5");
        Assert.assertTrue(jdbc.getOptions().connectTimeout == 5);
        Assert.assertTrue(jdbc.getOptions().validConnectionTimeout == 0);
        Assert.assertTrue(jdbc.getOptions().autoReconnect);
        Assert.assertTrue("root".equals(jdbc.getOptions().user));
    }

    @Test(expected = SQLException.class )
    public void testOptionParseIntegerNotPossible() throws Throwable {
        JDBCUrl.parse("jdbc:mysql://localhost/test?user=root&autoReconnect=true&validConnectionTimeout=-2&connectTimeout=5");
        Assert.fail();
    }

    @Test()
    public void testJDBCParserSimpleIPv4basic() throws SQLException {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
    }
    @Test
    public void testJDBCParserSimpleIPv4basicError() throws SQLException  {
        JDBCUrl jdbcUrl = JDBCUrl.parse(null);
        Assert.assertTrue(jdbcUrl == null);
    }
    @Test
    public void testJDBCParserSimpleIPv4basicwithoutDatabase() throws SQLException  {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        Assert.assertNull(jdbcUrl.getDatabase());
        Assert.assertNull(jdbcUrl.getUsername());
        Assert.assertNull(jdbcUrl.getPassword());
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(jdbcUrl.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(jdbcUrl.getHostAddresses().get(2)));
    }

    @Test
    public void testJDBCParserSimpleIPv4Properties() throws SQLException  {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database?autoReconnect=true";
        Properties prop = new Properties();
        prop.setProperty("user","greg");
        prop.setProperty("password","pass");

        JDBCUrl jdbcUrl = JDBCUrl.parse(url, prop);
        Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
        Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
        Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
        Assert.assertTrue(jdbcUrl.getOptions().autoReconnect);
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(jdbcUrl.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(jdbcUrl.getHostAddresses().get(2)));
    }

    @Test
    public void testJDBCParserSimpleIPv4() throws SQLException  {
        String url = "jdbc:mysql://master:3306,slave1:3307,slave2:3308/database?user=greg&password=pass";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
        Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
        Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master", 3306).equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3307).equals(jdbcUrl.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3308).equals(jdbcUrl.getHostAddresses().get(2)));
    }


    @Test
    public void testJDBCParserSimpleIPv6() throws SQLException  {
        String url = "jdbc:mysql://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306,[2001:660:7401:200::edf:bdd7]:3307/database?user=greg&password=pass";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
        Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
        Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 2);
        Assert.assertTrue(new HostAddress("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306).equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("2001:660:7401:200::edf:bdd7", 3307).equals(jdbcUrl.getHostAddresses().get(1)));
    }


    @Test
    public void testJDBCParserParameter()  throws SQLException {
        String url = "jdbc:mysql://address=(type=master)(port=3306)(host=master1),address=(port=3307)(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
        Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
        Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("master2", 3307, "master").equals(jdbcUrl.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave1", 3308, "slave").equals(jdbcUrl.getHostAddresses().get(2)));
    }

    @Test()
    public void testJDBCParserParameterErrorEqual() {
        String url = "jdbc:mysql://address=(type=)(port=3306)(host=master1),address=(port=3307)(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        try {
            JDBCUrl.parse(url);
            Assert.fail();
        }catch (SQLException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testJDBCParserHAModeNone() throws SQLException  {
        String url = "jdbc:mysql://localhost/database";
        JDBCUrl jdbc = JDBCUrl.parse(url);
        Assert.assertTrue(jdbc.getHaMode().equals(UrlHAMode.NONE));
    }

    @Test
    public void testJDBCParserHAModeLoadReplication() throws SQLException  {
        String url = "jdbc:mysql:replication://localhost/database";
        JDBCUrl jdbc = JDBCUrl.parse(url);
        Assert.assertTrue(jdbc.getHaMode().equals(UrlHAMode.REPLICATION));
    }

    @Test
    public void testJDBCParserReplicationParameter() throws SQLException  {
        String url = "jdbc:mysql:replication://address=(type=master)(port=3306)(host=master1),address=(port=3307)(type=master)(host=master2),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
        Assert.assertTrue("greg".equals(jdbcUrl.getUsername()));
        Assert.assertTrue("pass".equals(jdbcUrl.getPassword()));
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("master2", 3307, "master").equals(jdbcUrl.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave1", 3308, "slave").equals(jdbcUrl.getHostAddresses().get(2)));
    }

    @Test
    public void testJDBCParserReplicationParameterWithoutType() throws SQLException  {
        String url = "jdbc:mysql:replication://master1,slave1,slave2/database";
        JDBCUrl jdbcUrl = JDBCUrl.parse(url);
        Assert.assertTrue("database".equals(jdbcUrl.getDatabase()));
        Assert.assertTrue(jdbcUrl.getHostAddresses().size() == 3);
        Assert.assertTrue(new HostAddress("master1", 3306, "master").equals(jdbcUrl.getHostAddresses().get(0)));
        Assert.assertTrue(new HostAddress("slave1", 3306, "slave").equals(jdbcUrl.getHostAddresses().get(1)));
        Assert.assertTrue(new HostAddress("slave2", 3306, "slave").equals(jdbcUrl.getHostAddresses().get(2)));
    }

    @Test
    public void testJDBCParserHAModeLoadAurora() throws SQLException  {
        String url = "jdbc:mysql:aurora://localhost/database";
        JDBCUrl jdbc = JDBCUrl.parse(url);
        Assert.assertTrue(jdbc.getHaMode().equals(UrlHAMode.AURORA));
    }

    /**
     * CONJ-167 : Driver is throwing IllegalArgumentException instead of returning null
     */
    @Test
    public void checkOtherDriverCompatibility() throws SQLException  {
        String url = "jdbc:h2:mem:RZM;DB_CLOSE_DELAY=-1";
        JDBCUrl jdbc = JDBCUrl.parse(url);
        Assert.assertTrue(jdbc == null);
    }

}
