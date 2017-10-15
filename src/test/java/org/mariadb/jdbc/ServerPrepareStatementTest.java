/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.protocol.Protocol;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ServerPrepareStatementTest extends BaseTest {
    /**
     * Tables initialisations.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("ServerPrepareStatementTest", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementTestt", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementTestt2", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementTestCache", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementCacheSize3", "id int not null primary key auto_increment, test boolean");
        if (doPrecisionTest) {
            createTable("preparetestFactionnal", "time0 TIME(6) default '22:11:00', timestamp0 timestamp(6), datetime0 datetime(6) ");
        }
        createTable("ServerPrepareStatementCacheSize2", "id int not null primary key auto_increment, test boolean");
        createTable("ServerPrepareStatementCacheSize3", "id int not null primary key auto_increment, test blob");
        createTable("ServerPrepareStatementParameters", "id int, id2 int");
        createTable("ServerPrepareStatementCacheSize4", "id int not null primary key auto_increment, test LONGBLOB",
                "ROW_FORMAT=COMPRESSED ENGINE=INNODB");
        createTable("streamtest2", "id int primary key not null, strm text");
        createTable("testServerPrepareMeta", "id int not null primary key auto_increment, id2 int not null, id3 DEC(4,2), id4 BIGINT UNSIGNED ");
        createTable("ServerPrepareStatementSync", "id int not null primary key auto_increment, test varchar(1007), tt boolean");
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
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
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
            assertEquals(nbStatementCount + 1, rs.getInt(2));
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void serverCacheStatementTest() throws Throwable {
        Assume.assumeTrue(sharedUsePrepare());
        Connection connection = null;
        try {
            connection = setConnection();
            Protocol protocol = getProtocolFromConnection(connection);
            int cacheSize = protocol.prepareStatementCache().size();

            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO ServerPrepareStatementTestCache(test) VALUES (?)");
            preparedStatement.setBoolean(1, true);
            preparedStatement.execute();
            assertTrue(cacheSize + 1 == protocol.prepareStatementCache().size());

            PreparedStatement preparedStatement2 = connection.prepareStatement("INSERT INTO ServerPrepareStatementTestCache(test) VALUES (?)");
            preparedStatement2.setBoolean(1, true);
            preparedStatement2.execute();
            assertTrue(cacheSize + 1 == protocol.prepareStatementCache().size());
        } finally {
            if (connection != null) connection.close();
        }
    }

    @Test
    public void prepStmtCacheSize() throws Throwable {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);
        Connection connection = null;
        try {
            connection = setConnection("&prepStmtCacheSize=10");
            List<PreparedStatement> activePrepareStatement = new ArrayList<PreparedStatement>(20);
            for (int i = 0; i < 20; i++) {
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + i);
                preparedStatement.execute();
                activePrepareStatement.add(preparedStatement);
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

            assertEquals("ServerPrepareStatementCache.map[\n"
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
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + i);
                preparedStatement.execute();
                activePrepareStatement.add(preparedStatement);
            }

            assertEquals("ServerPrepareStatementCache.map[\n"
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
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + i);
                preparedStatement.execute();
                activePrepareStatement.add(preparedStatement);
            }
            assertEquals("ServerPrepareStatementCache.map[\n"
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
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + i);
                preparedStatement.execute();
                activePrepareStatement.add(preparedStatement);
            }
            assertEquals("ServerPrepareStatementCache.map[\n"
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
                PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + i);
                preparedStatement.execute();
                activePrepareStatement.add(preparedStatement);
            }
            assertEquals("ServerPrepareStatementCache.map[\n"
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
            assertEquals("ServerPrepareStatementCache.map[\n"
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
            if (connection != null) connection.close();
        }
    }

    /**
     * CONJ-290 : Timestamps format error when using prepareStatement with options useFractionalSeconds and useServerPrepStmts.
     *
     * @throws SQLException exception
     */
    @Test
    public void timeFractionnalSecondTest() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
        Connection connection = null;
        try {
            connection = setConnection("&useFractionalSeconds=false");
            Time time0 = new Time(55549392);
            Time time1 = new Time(55549000);

            Timestamp timestamp0 = new Timestamp(55549392);
            Timestamp timestamp1 = new Timestamp(55549000);


            PreparedStatement ps = connection.prepareStatement("INSERT INTO preparetestFactionnal (time0, timestamp0, datetime0) VALUES (?, ?, ?)");
            ps.setTime(1, time0);
            ps.setTimestamp(2, timestamp0);
            ps.setTimestamp(3, timestamp0);
            ps.addBatch();
            ps.setTime(1, time1);
            ps.setTimestamp(2, timestamp1);
            ps.setTimestamp(3, timestamp1);
            ps.addBatch();
            ps.executeBatch();

            ResultSet rs = connection.createStatement().executeQuery("SELECT * from preparetestFactionnal");
            if (rs.next()) {
                //must be equal to time1 and not time0
                assertEquals(rs.getTime(1), time1);
                assertEquals(rs.getTimestamp(2), timestamp1);
                assertEquals(rs.getTimestamp(3), timestamp1);
                assertTrue(rs.next());
                assertEquals(rs.getTime(1), time1);
                assertEquals(rs.getTimestamp(2), timestamp1);
                assertEquals(rs.getTimestamp(3), timestamp1);
            } else {
                fail("Error in query");
            }
        } finally {
            if (connection != null) connection.close();
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
                        + "timestamp1 TIMESTAMP(0) null default  '2001-01-01 00:00:00',"
                        + "timestamp_zero TIMESTAMP  null, "
                        + "time0 TIME(6) default '22:11:00',"
                        + ((!isMariadbServer() && minVersion(5, 6)) ? "year2 YEAR(4) default 99," : "year2 YEAR(2) default 99,")
                        + "year4 YEAR(4) default 2011,"
                        + "char0 CHAR(1) default '0',"
                        + "char_binary CHAR (1) binary default '0',"
                        + "varchar0 VARCHAR(1) default '1',"
                        + "varchar_binary VARCHAR(10) BINARY default 0x1,"
                        + "binary0 BINARY(10) default 0x1,"
                        + "varbinary0 VARBINARY(10) default 0x1, "
                        + "id int not null AUTO_INCREMENT, PRIMARY KEY (id)"
        );
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void dataConformity() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
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
            ps.setObject(26, char0, Types.CHAR);
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
            ResultSet rs = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
                    .executeQuery("SELECT * from preparetest");
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
                assertEquals(new String(rs.getBytes(30), Buffer.UTF_8),
                        new String(binary0, Buffer.UTF_8));
                assertEquals(new String(rs.getBytes(31), Buffer.UTF_8),
                        new String(varbinary0, Buffer.UTF_8));
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
                assertEquals(rs.getInt(fieldNumber), comparaison + 2000);
            } else {
                assertEquals(rs.getInt(fieldNumber), comparaison);
            }
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
            InputStream input = classLoader.getResourceAsStream("logback-test.xml");

            ps.setBlob(1, input);
            ps.addBatch();
            ps.executeBatch();
        } finally {
            if (connection != null) connection.close();
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
            if (connection != null) connection.close();
        }
    }

    @Test(expected = SQLException.class)
    public void parametersNotSetTest() throws Throwable {
        Assume.assumeTrue(sharedUsePrepare());
        PreparedStatement ps = sharedConnection.prepareStatement(
                "INSERT INTO ServerPrepareStatementParameters(id, id2) VALUES (?,?)");
        ps.setInt(1, 1);
        ps.addBatch();
        ps.executeBatch();
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
        ps.executeBatch();
    }

    @Test
    public void blobMultipleSizeTest() throws Throwable {
        Assume.assumeTrue(checkMaxAllowedPacketMore40m("blobMultipleSizeTest"));
        Assume.assumeTrue(sharedUsePrepare());

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
        assertTrue(rs.next());
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
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
    }

    @Test
    public void executeBatchNumber() throws Throwable {
        PreparedStatement ps = null;
        try {
            ps = prepareInsert();
            ps.executeBatch();
            ResultSet rs = ps.executeQuery("select count(*) from ServerPrepareStatementParameters");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
        } finally {
            ps.close();
        }
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
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
    }


    @Test
    public void dataConformity2() throws SQLException {
        Assume.assumeTrue(doPrecisionTest);
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
        Date date0 = Date.valueOf("2016-02-01");
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

            assertEquals(new String(rs.getBytes(30), Buffer.UTF_8),
                    new String(binary0, Buffer.UTF_8));
            assertEquals(new String(rs.getBytes(31), Buffer.UTF_8),
                    new String(varbinary0, Buffer.UTF_8));
        } else {
            fail();
        }

    }

    @Test
    public void testPrepareStatementCache() throws Throwable {
        Assume.assumeTrue(sharedOptions().useServerPrepStmts);

        //tester le cache prepareStatement
        Connection connection = null;
        try {
            connection = setConnection();
            Protocol protocol = getProtocolFromConnection(connection);
            createTable("test_cache_table1", "id1 int auto_increment primary key, text1 varchar(20), text2 varchar(20)");
            PreparedStatement[] map = new PreparedStatement[280];
            for (int i = 0; i < 280; i++) {
                map[i] = connection.prepareStatement(
                        "INSERT INTO test_cache_table1 (text1, text2) values (" + i + ", ?)");
                map[i].setString(1, i + "");
                map[i].setString(2, i + "");
                map[i].addBatch();
                map[i].executeBatch();

                if (i < 250) {
                    assertEquals(i + 1, protocol.prepareStatementCache().size());
                } else {
                    assertEquals(250, protocol.prepareStatementCache().size());
                }
            }
        } finally {
            if (connection != null) connection.close();
        }
    }

    /**
     * CONJ-270 : permit to have more than 32768 parameters.
     *
     * @throws SQLException exception
     */
    @Test
    public void testRewriteMultiPacket() throws SQLException {
        createTable("PreparedStatementTest3", "id int");
        StringBuilder sql = new StringBuilder("INSERT INTO PreparedStatementTest3 VALUES (?)");
        for (int i = 1; i < 65535; i++) {
            sql.append(",(?)");
        }
        PreparedStatement pstmt = sharedConnection.prepareStatement(sql.toString());
        for (int i = 1; i < 65536; i++) {
            pstmt.setInt(i, i);
        }
        assertFalse(pstmt.execute());
    }

    /**
     * Test that getGeneratedKey got the right insert ids values, even when batch in multiple queries (for rewrite).
     *
     * @throws SQLException if connection error occur
     */
    @Test
    public void serverPrepareStatementSync() throws Throwable {
        Assume.assumeTrue(!checkMaxAllowedPacketMore20m("serverPrepareStatementSync", false) && sharedIsRewrite()); // to avoid
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("select @@max_allowed_packet");
        if (rs.next()) {
            long maxAllowedPacket = rs.getInt(1);
            int totalInsertCommands = (int) Math.ceil(3 * maxAllowedPacket / 1000); //mean that there will be 2 commands
            Connection connection2 = null;
            try {
                connection2 = setConnection();
                PreparedStatement preparedStatement = sharedConnection.prepareStatement(
                        "INSERT INTO ServerPrepareStatementSync(test, tt) values (?, false) ", Statement.RETURN_GENERATED_KEYS);
                PreparedStatement preparedStatement2 = connection2.prepareStatement(
                        "INSERT INTO ServerPrepareStatementSync(test, tt) values (?, true) ", Statement.RETURN_GENERATED_KEYS);
                char[] thousandChars = new char[1000];
                Arrays.fill(thousandChars, 'a');
                String thousandLength = new String(thousandChars);

                for (int counter = 0; counter < totalInsertCommands + 1; counter++) {
                    preparedStatement.setString(1, "a" + counter + "_" + thousandLength);
                    preparedStatement.addBatch();
                    preparedStatement2.setString(1, "b" + counter + "_" + thousandLength);
                    preparedStatement2.addBatch();
                }

                ExecutorService executor = Executors.newFixedThreadPool(2);
                BatchThread thread1 = new BatchThread(preparedStatement);
                BatchThread thread2 = new BatchThread(preparedStatement2);
                executor.execute(thread1);
                //Thread.sleep(500);
                executor.execute(thread2);
                executor.shutdown();
                executor.awaitTermination(400, TimeUnit.SECONDS);
                ResultSet rs1 = preparedStatement.getGeneratedKeys();
                ResultSet rs2 = preparedStatement2.getGeneratedKeys();

                ResultSet rs3 = sharedConnection.createStatement().executeQuery("select id, tt from ServerPrepareStatementSync");
                while (rs3.next()) {
                    if (rs3.getBoolean(2)) {
                        rs2.next();
                        if (rs3.getInt(1) != rs2.getInt(1)) {
                            System.out.println("1 : " + rs3.getInt(1) + " != " + rs2.getInt(1));
                            fail();
                        }
                    } else {
                        rs1.next();
                        if (rs3.getInt(1) != rs1.getInt(1)) {
                            System.out.println("0 : " + rs3.getInt(1) + " != " + rs1.getInt(1));
                            fail();
                        }
                    }
                }
            } finally {
                connection2.close();
            }
        } else {
            fail();
        }
    }

    public static class BatchThread implements Runnable {
        private final PreparedStatement preparedStatement;

        BatchThread(PreparedStatement preparedStatement) {
            this.preparedStatement = preparedStatement;
        }

        @Override
        public void run() {
            try {
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Binary state reading control.
     *
     * @throws SQLException if connection error occur
     */
    @Test
    public void ensureRowStateWithNullValues() throws SQLException {
        createTable("ensureRowStateWithNullValues", "t1 varchar(20), t2 varchar(20), t3 varchar(20), t4 varchar(20), t5 varchar(20), t6 varchar(20)");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO ensureRowStateWithNullValues VALUES ('12345678901234567890', null, 'otherString', '1234567890', null, '12345')");
        PreparedStatement ps = sharedConnection.prepareStatement("SELECT * FROM ensureRowStateWithNullValues");
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("12345678901234567890", rs.getString(1));
        assertNull(rs.getString(2));
        assertNull(rs.getString(5));
        assertEquals("12345", rs.getString(6));

        assertFalse(rs.next());
    }


    /**
     * Binary state reading control - second part.
     *
     * @throws SQLException if connection error occur
     */
    @Test
    public void ensureRowStateWithNullValuesSecond() throws Exception {
        createTable("ensureRowStateWithNullValuesSecond",
                " ID int(11) NOT NULL,"
                        + " COLUMN_1 varchar(11) COLLATE utf8_bin DEFAULT NULL,"
                        + " COLUMN_2 varchar(11) COLLATE utf8_bin DEFAULT NULL,"
                        + " COLUMN_3 varchar(11) COLLATE utf8_bin DEFAULT NULL,"
                        + " PRIMARY KEY (ID)",
                "ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin");
        if (testSingleHost) {
            Statement st = sharedConnection.createStatement();
            st.execute("INSERT INTO ensureRowStateWithNullValuesSecond VALUES(1,'col 1 value', 'col 2 value', null)");
        }

        String sql = "SELECT ID, COLUMN_2, COLUMN_1, COLUMN_3 FROM ensureRowStateWithNullValuesSecond";
        Connection tmpConnection = null;
        try {
            tmpConnection = setConnection("&profileSql=true&useServerPrepStmts=true");
            Statement stmt = tmpConnection.createStatement();
            stmt.setQueryTimeout(1);
            stmt.execute(sql);

            final PreparedStatement preparedStatement = tmpConnection.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery();
            assertTrue(rs.next());

            String columnOne = rs.getString("COLUMN_1");
            String columnTwo = rs.getString("COLUMN_2");
            String columnThree = rs.getString("COLUMN_3");

            assertEquals("col 2 value", columnTwo);
            assertNull(columnThree);
            assertNotNull(columnOne);
            assertEquals("col 1 value", columnOne);

            columnThree = rs.getString("COLUMN_3");
            columnTwo = rs.getString("COLUMN_2");
            columnOne = rs.getString("COLUMN_1");

            assertEquals("col 2 value", columnTwo);
            assertNull(columnThree);
            assertNotNull(columnOne);
            assertEquals("col 1 value", columnOne);

            columnTwo = rs.getString("COLUMN_2");
            columnThree = rs.getString("COLUMN_3");
            columnOne = rs.getString("COLUMN_1");

            assertEquals("col 2 value", columnTwo);
            assertNull(columnThree);
            assertNotNull(columnOne);
            assertEquals("col 1 value", columnOne);
        } finally {
            tmpConnection.close();
        }
    }


}
