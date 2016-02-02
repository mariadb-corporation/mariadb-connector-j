package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ServerPrepareStatementTest extends BaseTest {
    /**
     * Tables initialisations.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("ServerPrepareStatementTest", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementTestt", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementTestCache", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementCacheSize3", "id int not null primary key auto_increment, test boolean");
        createTable("preparetestFactionnal", "time0 TIME(6) default '22:11:00'");
        createTable("ServerPrepareStatementCacheSize2", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementCacheSize3", "id int not null primary key auto_increment, test blob");
        createTable("ServerPrepareStatementParameters", "id int, id2 int");
        createTable("ServerPrepareStatementCacheSize4", "id int not null primary key auto_increment, test LONGBLOB",
                "ROW_FORMAT=COMPRESSED ENGINE=INNODB");
        createTable("streamtest2", "id int primary key not null, strm text");
        createTable("testServerPrepareMeta", "id int not null primary key auto_increment, id2 int not null, id3 DEC(4,2), id4 BIGINT UNSIGNED ");
        createTable("ServerPrepareStatementPrepareCache", "id int not null primary key auto_increment, test varchar(20)");
    }

    @Test
    public void testServerPrepareMeta() throws Throwable {
        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO testServerPrepareMeta(id2, id3, id4) VALUES (?, ?, ?)");
        ParameterMetaData meta = ps.getParameterMetaData();
        assertEquals(3, meta.getParameterCount());
    }

    @Test
    public void serverExecutionTest() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
            assertTrue(rs.next());
            final int nbStatementCount = rs.getInt(2);

            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO ServerPrepareStatementTestt (test) VALUES (?)");
            ps.setBoolean(1, true);
            ps.addBatch();
            ps.execute();

            rs = statement.executeQuery("show global status like 'Prepared_stmt_count'");
            assertTrue(rs.next());
            assertTrue(rs.getInt(2) == nbStatementCount + 1);
        } finally {
            connection.close();
        }
    }

    @Test
    public void serverCacheStatementTest() throws Throwable {
        Protocol protocol = getProtocolFromConnection(sharedConnection);
        int cacheSize = protocol.prepareStatementCache().size();
        sharedConnection.prepareStatement("INSERT INTO ServerPrepareStatementTestCache(test) VALUES (?)");
        assertTrue(cacheSize + 1 == protocol.prepareStatementCache().size());
        sharedConnection.prepareStatement("INSERT INTO ServerPrepareStatementTestCache(test) VALUES (?)");
        assertTrue(cacheSize + 1 == protocol.prepareStatementCache().size());
    }

    @Test
    public void prepStmtCacheSizeTest() throws Throwable {
        Connection connection = null;
        try {
            connection = setConnection("&prepStmtCacheSize=10");
            List<PreparedStatement> activePrepareStatement = new ArrayList<>(20);
            for (int i = 0; i < 20; i++) {
                activePrepareStatement.add(connection.prepareStatement("SELECT " + i));
            }
            //check max cache size
            Protocol protocol = getProtocolFromConnection(connection);
            assertTrue("Prepared cache size must be 10", protocol.prepareStatementCache().size() == 10);

            //check all prepared statement worked even if not cached
            for (int i = 0; i < 20; i++) {
                activePrepareStatement.get(i).execute();
            }
            assertTrue(protocol.prepareStatementCache().size() == 10);
            while (!activePrepareStatement.isEmpty()) {
                activePrepareStatement.get(0).close();
                activePrepareStatement.remove(0);
            }
            //check that cache hold preparedStatement
            assertTrue("Prepared cache size must be 10", protocol.prepareStatementCache().size() == 10);

            assertEquals("PrepareStatementCache.map[\n"
                    + "testj-SELECT 10-0\n"
                    + "testj-SELECT 11-0\n"
                    + "testj-SELECT 12-0\n"
                    + "testj-SELECT 13-0\n"
                    + "testj-SELECT 14-0\n"
                    + "testj-SELECT 15-0\n"
                    + "testj-SELECT 16-0\n"
                    + "testj-SELECT 17-0\n"
                    + "testj-SELECT 18-0\n"
                    + "testj-SELECT 19-0]", protocol.prepareStatementCache().toString());

            for (int i = 12; i < 15; i++) {
                activePrepareStatement.add(connection.prepareStatement("SELECT " + i));
            }

            assertEquals("PrepareStatementCache.map[\n"
                    + "testj-SELECT 10-0\n"
                    + "testj-SELECT 11-0\n"
                    + "testj-SELECT 15-0\n"
                    + "testj-SELECT 16-0\n"
                    + "testj-SELECT 17-0\n"
                    + "testj-SELECT 18-0\n"
                    + "testj-SELECT 19-0\n"
                    + "testj-SELECT 12-1\n"
                    + "testj-SELECT 13-1\n"
                    + "testj-SELECT 14-1]", protocol.prepareStatementCache().toString());

            for (int i = 1; i < 5; i++) {
                activePrepareStatement.add(connection.prepareStatement("SELECT " + i));
            }
            assertEquals("PrepareStatementCache.map[\n"
                    + "testj-SELECT 17-0\n"
                    + "testj-SELECT 18-0\n"
                    + "testj-SELECT 19-0\n"
                    + "testj-SELECT 12-1\n"
                    + "testj-SELECT 13-1\n"
                    + "testj-SELECT 14-1\n"
                    + "testj-SELECT 1-1\n"
                    + "testj-SELECT 2-1\n"
                    + "testj-SELECT 3-1\n"
                    + "testj-SELECT 4-1]", protocol.prepareStatementCache().toString());
            for (int i = 12; i < 15; i++) {
                activePrepareStatement.add(connection.prepareStatement("SELECT " + i));
            }
            assertEquals("PrepareStatementCache.map[\n"
                    + "testj-SELECT 17-0\n"
                    + "testj-SELECT 18-0\n"
                    + "testj-SELECT 19-0\n"
                    + "testj-SELECT 1-1\n"
                    + "testj-SELECT 2-1\n"
                    + "testj-SELECT 3-1\n"
                    + "testj-SELECT 4-1\n"
                    + "testj-SELECT 12-2\n"
                    + "testj-SELECT 13-2\n"
                    + "testj-SELECT 14-2]", protocol.prepareStatementCache().toString());

            for (int i = 20; i < 30; i++) {
                activePrepareStatement.add(connection.prepareStatement("SELECT " + i));
            }
            assertEquals("PrepareStatementCache.map[\n"
                    + "testj-SELECT 20-1\n"
                    + "testj-SELECT 21-1\n"
                    + "testj-SELECT 22-1\n"
                    + "testj-SELECT 23-1\n"
                    + "testj-SELECT 24-1\n"
                    + "testj-SELECT 25-1\n"
                    + "testj-SELECT 26-1\n"
                    + "testj-SELECT 27-1\n"
                    + "testj-SELECT 28-1\n"
                    + "testj-SELECT 29-1]", protocol.prepareStatementCache().toString());

            //check all prepared statement worked even if not cached
            while (!activePrepareStatement.isEmpty()) {
                activePrepareStatement.get(0).execute();
                activePrepareStatement.get(0).close();
                activePrepareStatement.remove(0);
            }
            assertTrue(protocol.prepareStatementCache().size() == 10);
            assertEquals("PrepareStatementCache.map[\n"
                    + "testj-SELECT 20-0\n"
                    + "testj-SELECT 21-0\n"
                    + "testj-SELECT 22-0\n"
                    + "testj-SELECT 23-0\n"
                    + "testj-SELECT 24-0\n"
                    + "testj-SELECT 25-0\n"
                    + "testj-SELECT 26-0\n"
                    + "testj-SELECT 27-0\n"
                    + "testj-SELECT 28-0\n"
                    + "testj-SELECT 29-0]", protocol.prepareStatementCache().toString());


        } finally {
            connection.close();
        }
    }


    @Test
    public void timeFractionnalSecondTest() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection("&useFractionalSeconds=false");
            Time time0 = new Time(55549392);
            Time time1 = new Time(55549000);

            PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetestFactionnal (time0) VALUES (?)");
            ps.setTime(1, time0);
            ps.addBatch();
            ps.setTime(1, time1);
            ps.addBatch();
            ps.executeBatch();

            ResultSet rs = connection.createStatement().executeQuery("SELECT * from preparetestFactionnal");
            if (rs.next()) {
                //must be equal to time1 and not time0
                assertEquals(rs.getTime(1), time1);
                rs.next();
                assertEquals(rs.getTime(1), time1);
            } else {
                fail("Error in query");
            }
        } finally {
            connection.close();
        }

    }

    private void prepareTestTable() throws SQLException {

        createTable("preparetest",
                "bit1 BIT(1),"
                        + "bit2 BIT(2),"
                        + "tinyint1 TINYINT(1),"
                        + "tinyint2 TINYINT(2),"
                        + "bool0 BOOL default 1,"
                        + "smallint0 SMALLINT default 1,"
                        + "smallint_unsigned SMALLINT UNSIGNED default 0,"
                        + "mediumint0 MEDIUMINT default 1,"
                        + "mediumint_unsigned MEDIUMINT UNSIGNED default 0,"
                        + "int0 INT default 1,"
                        + "int_unsigned INT UNSIGNED default 0,"
                        + "bigint0 BIGINT default 1,"
                        + "bigint_unsigned BIGINT UNSIGNED default 0,"
                        + "float0 FLOAT default 0,"
                        + "double0 DOUBLE default 1,"
                        + "decimal0 DECIMAL default 0,"
                        + "decimal1 DECIMAL(15,4) default 0,"
                        + "date0 DATE default '2001-01-01',"
                        + "datetime0 DATETIME(6) default '2001-01-01 00:00:00',"
                        + "timestamp0 TIMESTAMP(6) default  '2001-01-01 00:00:00',"
                        + "timestamp1 TIMESTAMP(0) default  '2001-01-01 00:00:00',"
                        + "timestamp_zero TIMESTAMP  null, "
                        + "time0 TIME(6) default '22:11:00',"
                        + ((!isMariadbServer() && minVersion(5, 6)) ? "year2 YEAR(4) default 99," : "year2 YEAR(2) default 99,")
                        + "year4 YEAR(4) default 2011,"
                        + "char0 CHAR(1) default '0',"
                        + "char_binary CHAR (1) binary default '0',"
                        + "varchar0 VARCHAR(1) default '1',"
                        + "varchar_binary VARCHAR(10) BINARY default 0x1,"
                        + "binary0 BINARY(10) default 0x1,"
                        + "varbinary0 VARBINARY(10) default 0x1"
        );
    }

    @Test
    public void dataConformityTest() throws SQLException {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            prepareTestTable();
            PreparedStatement ps = sharedConnection.prepareStatement("INSERT INTO preparetest (bit1,bit2,tinyint1,"
                    + "tinyint2,bool0,smallint0,smallint_unsigned,mediumint0,mediumint_unsigned,int0,"
                    + "int_unsigned,bigint0,bigint_unsigned, float0, double0, decimal0,decimal1, date0,datetime0, "
                    + "timestamp0,timestamp1,timestamp_zero, time0,"
                    + "year2,year4,char0, char_binary, varchar0, varchar_binary, binary0, varbinary0)  "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,"
                    + "?,?,?,?,?,?,?,?,?,?,?,?,?,"
                    + "?,?,?,?,?,?,?,?)");
            sharedConnection.createStatement().execute("truncate preparetest");

            boolean bit1 = Boolean.FALSE;
            ps.setBoolean(1, bit1);
            byte bit2 = (byte) 3;
            ps.setByte(2, bit2);
            byte tinyint1 = (byte) 127;
            ps.setByte(3, tinyint1);
            short tinyint2 = 127;
            ps.setShort(4, tinyint2);
            boolean bool0 = Boolean.FALSE;
            ps.setBoolean(5, bool0);
            short smallint0 = 5;
            ps.setShort(6, smallint0);
            short smallintUnsigned = Short.MAX_VALUE;
            ps.setShort(7, smallintUnsigned);
            int mediumint0 = 55000;
            ps.setInt(8, mediumint0);
            int mediumintUnsigned = 55000;
            ps.setInt(9, mediumintUnsigned);
            int int0 = Integer.MAX_VALUE;
            ps.setInt(10, int0);
            int intUnsigned = Integer.MAX_VALUE;
            ps.setInt(11, intUnsigned);
            long bigint0 = 5000L;
            ps.setLong(12, bigint0);
            BigInteger bigintUnsigned = new BigInteger("3147483647");
            ps.setObject(13, bigintUnsigned);
            float float0 = 3147483647.7527F;
            ps.setFloat(14, float0);
            double double0 = 3147483647.8527D;
            ps.setDouble(15, double0);
            BigDecimal decimal0 = new BigDecimal("3147483647");
            ps.setBigDecimal(16, decimal0);
            BigDecimal decimal1 = new BigDecimal("3147483647.9527");
            ps.setBigDecimal(17, decimal1);
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+00:00"));
            Date date0 = new Date(1441238400000L);
            ps.setDate(18, date0);
            Timestamp datetime0 = new Timestamp(-2124690212000L);
            datetime0.setNanos(392005000);
            ps.setTimestamp(19, datetime0);
            Timestamp timestamp0 = new Timestamp(1441290349000L);
            timestamp0.setNanos(392005000);
            ps.setTimestamp(20, timestamp0);
            Timestamp timestamp1 = new Timestamp(1441290349000L);
            ps.setTimestamp(21, timestamp1);
            ps.setTimestamp(22, null);
            Time time0 = new Time(55549392);
            ps.setTime(23, time0);
            short year2 = 30;
            ps.setShort(24, year2);
            int year4 = 2050;
            ps.setInt(25, year4);
            String char0 = "\n";
            ps.setObject(26, char0, java.sql.Types.CHAR);
            String charBinary = "\n";
            ps.setString(27, charBinary);
            String varchar0 = "\b";
            ps.setString(28, varchar0);
            String varcharBinary = "\b";
            ps.setString(29, varcharBinary);
            byte[] binary0 = "1234567890".getBytes();
            ps.setBytes(30, binary0);
            byte[] varbinary0 = "azerty".getBytes();
            ps.setBytes(31, varbinary0);

            ps.addBatch();
            ps.executeBatch();
            ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * from preparetest");
            if (rs.next()) {
                assertEquals(rs.getBoolean(1), bit1);
                assertEquals(rs.getByte(2), bit2);
                assertEquals(rs.getByte(3), tinyint1);
                assertEquals(rs.getShort(4), tinyint2);
                assertEquals(rs.getBoolean(5), bool0);
                assertEquals(rs.getShort(6), smallint0);
                assertEquals(rs.getShort(7), smallintUnsigned);
                assertEquals(rs.getInt(8), mediumint0);
                assertEquals(rs.getInt(9), mediumintUnsigned);
                assertEquals(rs.getInt(10), int0);
                assertEquals(rs.getInt(11), intUnsigned);
                assertEquals(rs.getInt(12), bigint0);
                assertEquals(rs.getObject(13), bigintUnsigned);
                assertEquals(rs.getFloat(14), float0, 10000);
                assertEquals(rs.getDouble(15), double0, 10000);
                assertEquals(rs.getBigDecimal(16), decimal0);
                assertEquals(rs.getBigDecimal(17), decimal1);
                Calendar cc = new GregorianCalendar();
                cc.setTimeInMillis(date0.getTime());
                cc.setTimeInMillis(date0.getTime());
                assertEquals(rs.getDate(18), date0);
                assertEquals(rs.getTimestamp(19), datetime0);
                assertEquals(rs.getTimestamp(20), timestamp0);
                assertEquals(rs.getTimestamp(21), timestamp1);
                assertNull(rs.getTimestamp(22));
                assertEquals(rs.getTime(23), time0);
                assertYear(rs, 24, year2);
                assertEquals(rs.getInt(25), year4);
                assertEquals(rs.getString(26), char0);
                assertEquals(rs.getString(27), charBinary);
                assertEquals(rs.getString(28), varchar0);
                assertEquals(rs.getString(29), varcharBinary);
                assertEquals(new String(rs.getBytes(30), StandardCharsets.UTF_8),
                        new String(binary0, StandardCharsets.UTF_8));
                assertEquals(new String(rs.getBytes(31), StandardCharsets.UTF_8),
                        new String(varbinary0, StandardCharsets.UTF_8));
            } else {
                fail();
            }

        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    private void assertYear(ResultSet rs, int fieldNumber, int comparaison) throws SQLException {
        if (isMariadbServer()) {
            assertEquals(rs.getInt(fieldNumber), comparaison);
        } else {
            if (minVersion(5, 6)) {
                //year on 2 bytes is deprecated since 5.5.27
                assertEquals(rs.getInt(fieldNumber), 2030);
            } else {
                assertEquals(rs.getInt(fieldNumber), 30);
            }
        }
    }

    @Test
    public void checkReusability() throws Throwable {
        setConnection("&prepStmtCacheSize=10");

        ExecutorService exec = Executors.newFixedThreadPool(2);

        //check blacklist shared
        exec.execute(new CreatePrepareDouble("INSERT INTO ServerPrepareStatementCacheSize2( test) VALUES (?)",
                sharedConnection, 100, 100));
        exec.execute(new CreatePrepareDouble("INSERT INTO ServerPrepareStatementCacheSize2( test) VALUES (?)",
                sharedConnection, 500, 100));
        //wait for thread endings
        exec.shutdown();
        try {
            exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            //eat exception
        }
    }

    @Test
    public void blobTest() throws Throwable {
        Connection connection = null;
        try {
            connection = setConnection("&prepStmtCacheSize=10");

            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream input = classLoader.getResourceAsStream("logback.xml");

            ps.setBlob(1, input);
            ps.addBatch();
            ps.executeBatch();
        } finally {
            connection.close();
        }
    }

    @Test
    public void readerTest() throws Throwable {
        Connection connection = null;
        try {
            connection = setConnection("&prepStmtCacheSize=10");
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO ServerPrepareStatementCacheSize3(test) VALUES (?)");
            Reader reader = new BufferedReader(new InputStreamReader(
                    ClassLoader.getSystemResourceAsStream("style.xml")));

            ps.setCharacterStream(1, reader);
            ps.addBatch();
            ps.executeBatch();
        } finally {
            connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void parametersNotSetTest() throws Throwable {
        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setInt(1, 1);
        ps.addBatch();
    }

    @Test
    public void checkSendDifferentParameterTypeTest() throws Throwable {
        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setByte(1, (byte) 1);
        ps.setShort(2, (short) 1);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        ps.execute();
    }

    @Test
    public void blobMultipleSizeTest() throws Throwable {
        Assume.assumeTrue(checkMaxAllowedPacketMore40m("blobMultipleSizeTest"));

        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO ServerPrepareStatementCacheSize4(test) VALUES (?)");
        byte[] arr = new byte[20000000];
        Arrays.fill(arr, (byte) 'b');
        InputStream input = new ByteArrayInputStream(arr);
        InputStream input2 = new ByteArrayInputStream(arr);
        InputStream input3 = new ByteArrayInputStream(arr);

        ps.setBlob(1, input);
        ps.addBatch();
        ps.setBlob(1, input2);
        ps.addBatch();
        ps.setBlob(1, input3);
        ps.addBatch();
        ps.executeBatch();

        Statement statement = sharedConnection.createStatement();
        ResultSet rs = statement.executeQuery("select * from ServerPrepareStatementCacheSize4");
        rs.next();
        byte[] newBytes = rs.getBytes(2);
        assertEquals(arr.length, newBytes.length);
        for (int i = 0; i < arr.length; i++) {
            assertEquals(arr[i], newBytes[i]);
        }
    }

    @Test
    public void executeNumber() throws Throwable {
        PreparedStatement ps = prepareInsert();
        ps.execute();
        ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
        rs.next();
        assertEquals(rs.getInt(1), 1);
    }

    @Test
    public void executeBatchNumber() throws Throwable {
        PreparedStatement ps = prepareInsert();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
        rs.next();
        assertEquals(rs.getInt(1), 3);
    }

    private PreparedStatement prepareInsert() throws Throwable {
        Statement statement = sharedConnection.createStatement();
        statement.execute("truncate ServerPrepareStatementParameters");
        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setByte(1, (byte) 1);
        ps.setShort(2, (short) 1);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        ps.setInt(1, Integer.MIN_VALUE);
        ps.setInt(2, Integer.MAX_VALUE);
        ps.addBatch();
        return ps;
    }

    @Test
    public void directExecuteNumber() throws Throwable {
        sharedConnection.createStatement().execute("truncate ServerPrepareStatementParameters");
        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setByte(1, (byte) 1);
        ps.setShort(2, (short) 1);
        ps.execute();
        ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
        rs.next();
        assertEquals(rs.getInt(1), 1);
    }


    @Test
    public void dataConformityTest2() throws SQLException {
        prepareTestTable();

        PreparedStatement ps = sharedConnection.prepareStatement("INSERT INTO preparetest "
                + "(bit1,bit2,tinyint1,tinyint2,bool0,smallint0,smallint_unsigned,mediumint0,mediumint_unsigned,int0,"
                + "int_unsigned,bigint0,bigint_unsigned, float0, double0, decimal0,decimal1, date0,datetime0, "
                + "timestamp0,timestamp1,timestamp_zero, time0,"
                + "year2,year4,char0, char_binary, varchar0, varchar_binary, binary0, varbinary0)  "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?,?,?,?,?,?,"
                + "?,?,?,?,?,?,?,?)");
        boolean bit1 = Boolean.FALSE;
        ps.setBoolean(1, bit1);
        byte bit2 = (byte) 3;
        ps.setByte(2, bit2);
        byte tinyint1 = (byte) 127;
        ps.setByte(3, tinyint1);
        short tinyint2 = 127;
        ps.setShort(4, tinyint2);
        boolean bool0 = Boolean.FALSE;
        ps.setBoolean(5, bool0);
        short smallint0 = 5;
        ps.setShort(6, smallint0);
        short smallintUnsigned = Short.MAX_VALUE;
        ps.setShort(7, smallintUnsigned);
        int mediumint0 = 55000;
        ps.setInt(8, mediumint0);
        int mediumintUnsigned = 55000;
        ps.setInt(9, mediumintUnsigned);
        int int0 = Integer.MAX_VALUE;
        ps.setInt(10, int0);
        int intUnsigned = Integer.MAX_VALUE;
        ps.setInt(11, intUnsigned);
        long bigint0 = 5000L;
        ps.setLong(12, bigint0);
        BigInteger bigintUnsigned = new BigInteger("3147483647");
        ps.setObject(13, bigintUnsigned);
        float float0 = 3147483647.7527F;
        ps.setFloat(14, float0);
        double double0 = 3147483647.8527D;
        ps.setDouble(15, double0);
        BigDecimal decimal0 = new BigDecimal("3147483647");
        ps.setBigDecimal(16, decimal0);
        BigDecimal decimal1 = new BigDecimal("3147483647.9527");
        ps.setBigDecimal(17, decimal1);
        Date date0 = java.sql.Date.valueOf("2016-02-01");
        ps.setDate(18, date0);
        Timestamp datetime0 = new Timestamp(-2124690212000L);
        datetime0.setNanos(392005000);
        ps.setTimestamp(19, datetime0);
        Timestamp timestamp0 = new Timestamp(1441290349000L);
        timestamp0.setNanos(392005000);
        ps.setTimestamp(20, timestamp0);
        Timestamp timestamp1 = new Timestamp(1441290349000L);
        ps.setTimestamp(21, timestamp1);
        ps.setTimestamp(22, null);
        Time time0 = new Time(55549392);
        ps.setTime(23, time0);
        short year2 = 30;
        ps.setShort(24, year2);
        int year4 = 2050;
        ps.setInt(25, year4);
        String char0 = "\n";
        ps.setString(26, char0);
        String charBinary = "\n";
        ps.setString(27, charBinary);
        String varchar0 = "\b";
        ps.setString(28, varchar0);
        String varcharBinary = "\b";
        ps.setString(29, varcharBinary);
        byte[] binary0 = "1234567890".getBytes();
        ps.setBytes(30, binary0);
        byte[] varbinary0 = "azerty".getBytes();
        ps.setBytes(31, varbinary0);

        ps.addBatch();
        ps.executeBatch();

        PreparedStatement prepStmt = sharedConnection.prepareStatement("SELECT * from preparetest where bit1 = ?");
        prepStmt.setBoolean(1, false);
        ResultSet rs = prepStmt.executeQuery();
        if (rs.next()) {
            assertEquals(rs.getBoolean(1), bit1);
            assertEquals(rs.getByte(2), bit2);
            assertEquals(rs.getByte(3), tinyint1);
            assertEquals(rs.getShort(4), tinyint2);
            assertEquals(rs.getBoolean(5), bool0);
            assertEquals(rs.getShort(6), smallint0);
            assertEquals(rs.getShort(7), smallintUnsigned);
            assertEquals(rs.getInt(8), mediumint0);
            assertEquals(rs.getInt(9), mediumintUnsigned);
            assertEquals(rs.getInt(10), int0);
            assertEquals(rs.getInt(11), intUnsigned);
            assertEquals(rs.getInt(12), bigint0);
            assertEquals(rs.getObject(13), bigintUnsigned);
            assertEquals(rs.getFloat(14), float0, 10000);
            assertEquals(rs.getDouble(15), double0, 10000);
            assertEquals(rs.getBigDecimal(16), decimal0);
            assertEquals(rs.getBigDecimal(17), decimal1);
            assertEquals(rs.getDate(18), date0);
            assertEquals(rs.getTimestamp(19), datetime0);
            assertEquals(rs.getTimestamp(20), timestamp0);
            assertEquals(rs.getTimestamp(21), timestamp1);
            assertNull(rs.getTimestamp(22));
            assertEquals(rs.getTime(23), time0);
            assertYear(rs, 24, year2);
            assertEquals(rs.getInt(25), year4);
            assertEquals(rs.getString(26), char0);
            assertEquals(rs.getString(27), charBinary);
            assertEquals(rs.getString(28), varchar0);
            assertEquals(rs.getString(29), varcharBinary);

            assertEquals(new String(rs.getBytes(30), StandardCharsets.UTF_8),
                    new String(binary0, StandardCharsets.UTF_8));
            assertEquals(new String(rs.getBytes(31), StandardCharsets.UTF_8),
                    new String(varbinary0, StandardCharsets.UTF_8));
        } else {
            fail();
        }

    }

    protected class CreatePrepareDouble implements Runnable {
        private String sql;
        private Connection connection;
        private long firstWaitTime;
        private long secondWaitTime;


        public CreatePrepareDouble(String sql, Connection connection, long firstWaitTime, long secondWaitTime) {
            this.sql = sql;
            this.connection = connection;
            this.firstWaitTime = firstWaitTime;
            this.secondWaitTime = secondWaitTime;
        }

        public void run() {
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                if (protocol.prepareStatementCache().containsKey(sql)) {
                    protocol.prepareStatementCache().get(sql);
                }
                if (protocol.prepareStatementCache().containsKey(sql)) {
                    protocol.prepareStatementCache().get(sql);
                }
                PreparedStatement ps = connection.prepareStatement(sql);
                Thread.sleep(firstWaitTime);
                ps.setBoolean(1, true);
                ps.addBatch();
                ps.executeBatch();
                Thread.sleep(secondWaitTime);
                ps.close();
                if (protocol.prepareStatementCache().containsKey(sql)) {
                    protocol.prepareStatementCache().get(sql);
                }
            } catch (Throwable e) {
                fail();
            }
        }
    }

    @Test
    public void testCache() throws SQLException {
        try (Connection connection = setConnection()) {
            final String query = "INSERT INTO ServerPrepareStatementPrepareCache(test) VALUES (?)";
            final long startTime = System.nanoTime();
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, "test1");
            pstmt.execute();
            final long executionTime = System.nanoTime() - startTime;

            final long startTimeSecond = System.nanoTime();
            PreparedStatement pstmt2 = connection.prepareStatement(query);
            pstmt2.setString(1, "test2");
            pstmt2.execute();
            final long executionTimeSecond = System.nanoTime() - startTimeSecond;

            System.out.println("total time : " + (executionTimeSecond) + " first : " + executionTime);
            Assert.assertTrue(executionTimeSecond  < executionTime);

            ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM ServerPrepareStatementPrepareCache");
            if (resultSet.next()) {
                Assert.assertEquals("test1", resultSet.getString(2));
                if (resultSet.next()) {
                    Assert.assertEquals("test2", resultSet.getString(2));
                } else {
                    Assert.fail("Must have a result");
                }
            } else {
                Assert.fail("Must have a result");
            }
        }
    }



    @Test
    public void testPrepareStatementCache() throws Throwable {
        //tester le cache prepareStatement
        try (Connection connection = setConnection()) {
            MasterProtocol protocol = (MasterProtocol) getProtocolFromConnection(connection);
            createTable("test_cache_table1", "id1 int auto_increment primary key, text1 varchar(20), text2 varchar(20)");
            PreparedStatement[] map = new PreparedStatement[280];
            for (int i = 0; i < 280; i++) {
                map[i] = connection.prepareStatement(
                        "INSERT INTO test_cache_table1 (text1, text2) values (" + i + ", ?)");
                if (i < 250) {
                    Assert.assertEquals(i + 1, protocol.prepareStatementCache().size());
                } else {
                    Assert.assertEquals(250, protocol.prepareStatementCache().size());
                }
            }
        }
    }
}
