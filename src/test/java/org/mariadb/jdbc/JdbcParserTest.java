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
        assertEquals(jdbc.getDatabase(), jdbc2.getDatabase());
        assertEquals(jdbc.getOptions(), jdbc2.getOptions());
        assertEquals(jdbc.getHostAddresses(), jdbc2.getHostAddresses());
        assertEquals(jdbc.getHaMode(), jdbc2.getHaMode());
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
        assertTrue(jdbc.getOptions().validConnectionTimeout == 0);
        assertFalse(jdbc.getOptions().autoReconnect);
        assertNull(jdbc.getOptions().user);
        assertFalse(jdbc.getOptions().createDatabaseIfNotExist);
        assertNull(jdbc.getOptions().socketTimeout);

    }

    @Test
    public void testOptionTakeDefaultAurora() throws Throwable {
        UrlParser jdbc = UrlParser.parse("jdbc:mariadb:aurora://cluster-identifier.cluster-customerID.region.rds.amazonaws.com/test");
        assertNull(jdbc.getOptions().connectTimeout);
        assertTrue(jdbc.getOptions().validConnectionTimeout == 0);
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

    /**
     * CONJ-452 : correcting line break in connection url.
     * @throws SQLException if any exception occur
     */
    @Test
    public void checkInfileCertificate() throws SQLException {
        String url = "jdbc:mariadb://1.2.3.4/testj?user=diego"
                + "&profileSql=true&serverSslCert="
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDITCCAgmgAwIBAgIBADANBgkqhkiG9w0BAQUFADBIMSMwIQYDVQQDExpHb29n\n"
                + "bGUgQ2xvdWQgU1FMIFNlcnZlciBDQTEUMBIGA1UEChMLR29vZ2xlLCBJbmMxCzAJ\n"
                + "BgNVBAYTAlVTMB4XDTE3MDQyNzEyMjcyNFoXDTE5MDQyNzEyMjgyNFowSDEjMCEG\n"
                + "A1UEAxMaR29vZ2xlIENsb3VkIFNRTCBTZXJ2ZXIgQ0ExFDASBgNVBAoTC0dvb2ds\n"
                + "ZSwgSW5jMQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBANA6vS37//gvYOsEXKA9Cnlt/C1Ef/a5zJNahpVxx8HbJn6DF+pQbhHm3o4P\n"
                + "TeZp1HoRg5TRiXOEkNTBmgSQbR2+otM2q2gmkn4XAh0+yXkNW3hr2IydJyg9C26v\n"
                + "/OzFvuLcw9iDBvrn433pDa6vjYDU+wiQaVtr1ItzsoE/kgW2IkgFVQB+CrkpAmwm\n"
                + "omwEze3QFUUznP0PHy3P7g7UVD9u5x3APY6kVt2dq8mnOiLZkyfHHR2j6+j0E73I\n"
                + "k3HQv7D0yRIv3kuNpFgJbITVgDIq9ukWU2XinDHUjguCDH+yQAoQH7hOQlWUHIz8\n"
                + "/TtfZjrlUQf2uLzOWCn5KxfEqTkCAwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIB\n"
                + "ADANBgkqhkiG9w0BAQUFAAOCAQEArYkBkUvMvteT4fN6lxUkzmp8R7clLPkA2HnJ\n"
                + "7IUK3c5GJ0/blffxj/9Oe2g+etga15SIO73GNAnTxhxIJllKRmmh6SR+dwNMkAvE\n"
                + "xp87/Y6cSeJp5d4HhZUvxpFjaUDsWIC8tpbriUJoqGIirprLVcsPgDjKyuvVOlbK\n"
                + "aQf3fOoBPLspGWHgic8Iw1O4kRcStUGCSCwOtYcgMJEhVqTgX0sTX5BgatZhr8FY\n"
                + "Mnoceo2vzzxgHJU9qZuPkpYDs+ipQjzhoIJaY4HU2Uz4jMptqxSdzsPpC6PAKwuN\n"
                + "+LBCR0B194YbRn6726vWwUUE05yskVN6gllGSCgZ/G8y98DhjQ==\n"
                + "-----END CERTIFICATE-----&useSSL=true&password=testj&password=pwd2";
        UrlParser jdbc = UrlParser.parse(url);
        assertEquals("diego", jdbc.getOptions().user);
        assertEquals(true, jdbc.getOptions().profileSql);
        assertEquals("-----BEGIN CERTIFICATE-----\n"
                + "MIIDITCCAgmgAwIBAgIBADANBgkqhkiG9w0BAQUFADBIMSMwIQYDVQQDExpHb29n\n"
                + "bGUgQ2xvdWQgU1FMIFNlcnZlciBDQTEUMBIGA1UEChMLR29vZ2xlLCBJbmMxCzAJ\n"
                + "BgNVBAYTAlVTMB4XDTE3MDQyNzEyMjcyNFoXDTE5MDQyNzEyMjgyNFowSDEjMCEG\n"
                + "A1UEAxMaR29vZ2xlIENsb3VkIFNRTCBTZXJ2ZXIgQ0ExFDASBgNVBAoTC0dvb2ds\n"
                + "ZSwgSW5jMQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBANA6vS37//gvYOsEXKA9Cnlt/C1Ef/a5zJNahpVxx8HbJn6DF+pQbhHm3o4P\n"
                + "TeZp1HoRg5TRiXOEkNTBmgSQbR2+otM2q2gmkn4XAh0+yXkNW3hr2IydJyg9C26v\n"
                + "/OzFvuLcw9iDBvrn433pDa6vjYDU+wiQaVtr1ItzsoE/kgW2IkgFVQB+CrkpAmwm\n"
                + "omwEze3QFUUznP0PHy3P7g7UVD9u5x3APY6kVt2dq8mnOiLZkyfHHR2j6+j0E73I\n"
                + "k3HQv7D0yRIv3kuNpFgJbITVgDIq9ukWU2XinDHUjguCDH+yQAoQH7hOQlWUHIz8\n"
                + "/TtfZjrlUQf2uLzOWCn5KxfEqTkCAwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIB\n"
                + "ADANBgkqhkiG9w0BAQUFAAOCAQEArYkBkUvMvteT4fN6lxUkzmp8R7clLPkA2HnJ\n"
                + "7IUK3c5GJ0/blffxj/9Oe2g+etga15SIO73GNAnTxhxIJllKRmmh6SR+dwNMkAvE\n"
                + "xp87/Y6cSeJp5d4HhZUvxpFjaUDsWIC8tpbriUJoqGIirprLVcsPgDjKyuvVOlbK\n"
                + "aQf3fOoBPLspGWHgic8Iw1O4kRcStUGCSCwOtYcgMJEhVqTgX0sTX5BgatZhr8FY\n"
                + "Mnoceo2vzzxgHJU9qZuPkpYDs+ipQjzhoIJaY4HU2Uz4jMptqxSdzsPpC6PAKwuN\n"
                + "+LBCR0B194YbRn6726vWwUUE05yskVN6gllGSCgZ/G8y98DhjQ==\n"
                + "-----END CERTIFICATE-----", jdbc.getOptions().serverSslCert);
        assertEquals(true, jdbc.getOptions().useSsl);
        assertEquals("testj", jdbc.getOptions().password);

    }


}