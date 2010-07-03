package org.drizzle.jdbc;

import org.junit.Test;
import org.junit.After;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.*;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.sql.*;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
public class DriverTest {
    public static String host = "10.100.100.50";
    private Connection connection;
    static { Logger.getLogger("").setLevel(Level.OFF); }

    public DriverTest() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:mysql:thin://10.100.100.50:3306/test_units_jdbc");
       connection = DriverManager.getConnection("jdbc:drizzle://"+host+":3307/test_units_jdbc");
       //connection = DriverManager.getConnection("jdbc:mysql://10.100.100.50:3306/test_units_jdbc");
    }
    @After
    public void close() throws SQLException {
        connection.close();
    }
    public Connection getConnection() {
        return connection;
    }
    @Test
    public void doQuery() throws SQLException{
        Statement stmt = getConnection().createStatement();
        try { stmt.execute("drop table t1"); } catch (Exception e) {}
        stmt.execute("create table t1 (id int not null primary key auto_increment, test varchar(20))");
        stmt.execute("insert into t1 (test) values (\"hej1\")");
        stmt.execute("insert into t1 (test) values (\"hej2\")");
        stmt.execute("insert into t1 (test) values (\"hej3\")");
        stmt.execute("insert into t1 (test) values (null)");
        ResultSet rs = stmt.executeQuery("select * from t1");
        for(int i=1;i<4;i++) {
            rs.next();
            assertEquals(String.valueOf(i),rs.getString(1));
            assertEquals("hej"+i,rs.getString("test"));
        }
        rs.next();
        assertEquals(null,rs.getString("test"));
    }
    @Test(expected = SQLException.class)
    public void askForBadColumnTest() throws SQLException{
        Statement stmt = getConnection().createStatement();
        try { stmt.execute("drop table t1"); } catch (Exception e) {}
        stmt.execute("create table t1 (id int not null primary key auto_increment, test varchar(20))");
        stmt.execute("insert into t1 (test) values (\"hej1\")");
        stmt.execute("insert into t1 (test) values (\"hej2\")");
        stmt.execute("insert into t1 (test) values (\"hej3\")");
        stmt.execute("insert into t1 (test) values (null)");
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.next();
        rs.getInt("non_existing_column");
    }
    @Test(expected = SQLException.class)
    public void askForBadColumnIndexTest() throws SQLException{

        Statement stmt = getConnection().createStatement();
        try { stmt.execute("drop table t1"); } catch (Exception e) {}
        stmt.execute("create table t1 (id int not null primary key auto_increment, test varchar(20))");
        stmt.execute("insert into t1 (test) values (\"hej1\")");
        stmt.execute("insert into t1 (test) values (\"hej2\")");
        stmt.execute("insert into t1 (test) values (\"hej3\")");
        stmt.execute("insert into t1 (test) values (null)");
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.next();
        rs.getInt(102);
    }

    @Test(expected = SQLException.class)
    public void badQuery() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeQuery("whraoaooa");
    }

    @Test
    public void intOperations() {
        byte [] a = WriteBuffer.intToByteArray(56*256*256*256 + 11*256*256 + 77*256 + 99);
        assertEquals(a[0],99);
        assertEquals(a[1],77);
        assertEquals(a[2],11);
        assertEquals(a[3],56);
    }
    @Test
    public void preparedTest() throws SQLException {
        String query = "SELECT * FROM t1 WHERE test = ? and id = ?";
        PreparedStatement prepStmt = getConnection().prepareStatement(query);
        prepStmt.setString(1,"hej1");
        prepStmt.setInt(2,1);
        ResultSet results = prepStmt.executeQuery();
        String res = "";
        while(results.next()) {
            res=results.getString("test");
        }
        assertEquals("hej1",res);
        assertEquals(2, prepStmt.getParameterMetaData().getParameterCount());
    }

    @Test
    public void preparedTestWithLimit() throws SQLException {
        PreparedStatement ps = getConnection().prepareStatement("select * from t1 limit ?, ?");
        ps.setBytes(1, "20".getBytes());
        ps.setObject(2, 30);
        ps.execute();
    }
    @Test
    public void preparedTest2() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeQuery("DROP TABLE IF EXISTS prep_test");
        stmt.executeQuery("CREATE TABLE prep_test (id int not null primary key auto_increment, test varchar(20)) engine=innodb");
        PreparedStatement prepStmt = getConnection().prepareStatement("insert into prep_test (test) values (?) ");
        for(int i=0;i<1000;i++) {
            prepStmt.setString(1,"mee");
            prepStmt.execute();
        }
    }

    @Test
    public void updateTest() throws SQLException {
        Statement stmt = getConnection().createStatement();
        try { stmt.execute("drop table t1"); } catch (Exception e) {}
        stmt.execute("create table t1 (id int not null primary key auto_increment, test varchar(20))");
        stmt.execute("insert into t1 (test) values (\"hej1\")");
        stmt.execute("insert into t1 (test) values (\"hej2\")");
        stmt.execute("insert into t1 (test) values (\"hej3\")");
        stmt.execute("insert into t1 (test) values (null)");

        String query = "UPDATE t1 SET test = ? where id = ?";
        PreparedStatement prepStmt = getConnection().prepareStatement(query);
        prepStmt.setString(1,"updated");
        prepStmt.setInt(2,3);
        int updateCount = prepStmt.executeUpdate();
        assertEquals(1,updateCount);
        String query2 = "SELECT * FROM t1 WHERE id=?";
        prepStmt = getConnection().prepareStatement(query2);
        prepStmt.setInt(1,3);
        ResultSet results = prepStmt.executeQuery();
        String result = "";
        while(results.next()) {
            result = results.getString("test");
        }
        assertEquals("updated",result);
    }
    @Test
    public void autoIncTest() throws SQLException {
        String query = "CREATE TABLE t2 (id int not null primary key auto_increment, test varchar(10))";
        Statement stmt = getConnection().createStatement();
        stmt.execute("DROP TABLE IF EXISTS t2");
        stmt.execute(query);
        stmt.execute("INSERT INTO t2 (test) VALUES ('aa')");
        ResultSet rs = stmt.getGeneratedKeys();
        if(rs.next()) {
            assertEquals(1,rs.getInt(1));
            assertEquals(1,rs.getInt("insert_id"));
        } else {
            throw new SQLException("Could not get generated keys");
        }
        stmt.execute("INSERT INTO t2 (test) VALUES ('aa')");
        rs = stmt.getGeneratedKeys();
        if(rs.next()) {
            assertEquals(2,rs.getInt(1));
            assertEquals(2,rs.getInt("insert_id"));
        } else {
            throw new SQLException("Could not get generated keys");
        }

    }
    @Test
    public void autoIncPrepStmtTest() throws SQLException {
        String query = "CREATE TABLE test_a_inc_prep_stmt (id int not null primary key auto_increment, test varchar(10))";
        Statement stmt = getConnection().createStatement();
        stmt.execute("DROP TABLE IF EXISTS test_a_inc_prep_stmt");
        stmt.execute(query);
        PreparedStatement ps = getConnection().prepareStatement("insert into test_a_inc_prep_stmt (test) values (?)");
        ps.setString(1,"test123");
        ps.execute();
        ResultSet rs = ps.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals(1,rs.getInt("insert_id"));
    }    
    @Test
    public void transactionTest() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeQuery("DROP TABLE IF EXISTS t3");
        stmt.executeQuery("CREATE TABLE t3 (id int not null primary key auto_increment, test varchar(20)) engine=innodb");
        getConnection().setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO t3 (test) VALUES ('heja')");
        stmt.executeUpdate("INSERT INTO t3 (test) VALUES ('japp')");
        getConnection().commit();
        ResultSet rs = stmt.executeQuery("SELECT * FROM t3");
        assertEquals(true,rs.next());
        assertEquals("heja",rs.getString("test"));
        assertEquals(true,rs.next());
        assertEquals("japp",rs.getString("test"));
        assertEquals(false, rs.next());
        stmt.executeUpdate("INSERT INTO t3 (test) VALUES ('rollmeback')");
        ResultSet rsGen = stmt.getGeneratedKeys();
        rsGen.next();
        assertEquals(3,rsGen.getInt(1));
        getConnection().rollback();
        rs = stmt.executeQuery("SELECT * FROM t3 WHERE id=3");
        assertEquals(false,rs.next());
    }
    @Test
    public void savepointTest() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeUpdate("drop table if exists t4");
        stmt.executeUpdate("create table t4 (id int not null primary key auto_increment, test varchar(20)) engine=innodb");
        getConnection().setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej1')");
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej2')");
        Savepoint savepoint = getConnection().setSavepoint("yep");
        stmt.executeUpdate("INSERT INTO t4 (test)  values('hej3')");
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej4')");
        getConnection().rollback(savepoint);
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej5')");
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej6')");
        getConnection().commit();
        ResultSet rs = stmt.executeQuery("SELECT * FROM t4");
        assertEquals(true, rs.next());
        assertEquals("hej1",rs.getString(2));
        assertEquals(true, rs.next());
        assertEquals("hej2",rs.getString(2));
        assertEquals(true, rs.next());
        assertEquals("hej5",rs.getString(2));
        assertEquals(true, rs.next());
        assertEquals("hej6",rs.getString(2));
        assertEquals(false,rs.next());
    }
    @Test
    public void isolationLevel() throws SQLException {
        getConnection().setTransactionIsolation(getConnection().TRANSACTION_READ_UNCOMMITTED);
        assertEquals(getConnection().TRANSACTION_READ_UNCOMMITTED,getConnection().getTransactionIsolation());
        getConnection().setTransactionIsolation(getConnection().TRANSACTION_READ_COMMITTED);
        assertEquals(getConnection().TRANSACTION_READ_COMMITTED,getConnection().getTransactionIsolation());
        getConnection().setTransactionIsolation(getConnection().TRANSACTION_SERIALIZABLE);
        assertEquals(getConnection().TRANSACTION_SERIALIZABLE,getConnection().getTransactionIsolation());
        getConnection().setTransactionIsolation(getConnection().TRANSACTION_REPEATABLE_READ);
        assertEquals(getConnection().TRANSACTION_REPEATABLE_READ,getConnection().getTransactionIsolation());
    }

    @Test
    public void isValidTest() throws SQLException {
        assertEquals(true,getConnection().isValid(0));
    }

    @Test
    public void conStringTest() throws SQLException {
        JDBCUrl url = JDBCUrl.parse("jdbc:drizzle://www.drizzle.org:4427/mmm");
        assertEquals("",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://www.drizzle.org:3306/mmm");
        assertEquals("",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = JDBCUrl.parse("jdbc:drizzle://whoa@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://whoa@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = JDBCUrl.parse("jdbc:drizzle://whoa:pass@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://whoa:pass@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = JDBCUrl.parse("jdbc:drizzle://whoa:pass@www.drizzle.org/aa");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("aa",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://whoa:pass@www.drizzle.org/aa");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("aa",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = JDBCUrl.parse("jdbc:drizzle://whoa:pass@www.drizzle.org/cc");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("cc",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://whoa:pass@www.drizzle.org/cc");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("cc",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = JDBCUrl.parse("jdbc:drizzle://whoa:pass@www.drizzle.org/bbb");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://whoa:pass@www.drizzle.org/bbb");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = JDBCUrl.parse("jdbc:drizzle://whoa:pass@www.drizzle.org/bbb/");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = JDBCUrl.parse("jdbc:mysql:thin://whoa:pass@www.drizzle.org/bbb/");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

    }

    @Test
    public void testEscapes() throws SQLException {
        String query = "select * from t1 where test = ?";
        PreparedStatement stmt = getConnection().prepareStatement(query);
        stmt.setString(1,"hej\"");
        ResultSet rs = stmt.executeQuery();
        assertEquals(false,rs.next());
    }

    @Test
    public void testPreparedWithNull() throws SQLException {
        String query = "insert into t1 (test) values (null)";
        PreparedStatement pstmt = getConnection().prepareStatement(query);
        pstmt.execute();
        query = "select * from t1 where test is ?";
        pstmt = getConnection().prepareStatement(query);
        pstmt.setNull(1,1);
        ResultSet rs = pstmt.executeQuery();
        assertEquals(true,rs.next());
        assertEquals(null,rs.getString("test"));
        assertEquals(true,rs.wasNull());
    }

    @Test
    public void batchTest() throws SQLException {
        getConnection().createStatement().executeQuery("drop table if exists test_batch");
        getConnection().createStatement().executeQuery("create table test_batch (id int not null primary key auto_increment, test varchar(10))");
        PreparedStatement ps = getConnection().prepareStatement("insert into test_batch values (null, ?)");
        ps.setString(1, "aaa");
        ps.addBatch();
        ps.setString(1, "bbb");
        ps.addBatch();
        ps.setString(1, "ccc");
        ps.addBatch();
        int [] a = ps.executeBatch();
        for(int c : a ) assertEquals(1,c);
        ps.setString(1, "aaa");
        ps.addBatch();
        ps.setString(1, "bbb");
        ps.addBatch();
        ps.setString(1, "ccc");
        ps.addBatch();
         a = ps.executeBatch();
        for(int c : a ) assertEquals(1,c);
        ResultSet rs = getConnection().createStatement().executeQuery("select * from test_batch");
        assertEquals(true,rs.next());
        assertEquals("aaa",rs.getString(2));
        assertEquals(true,rs.next());
        assertEquals("bbb",rs.getString(2));
        assertEquals(true,rs.next());
        assertEquals("ccc",rs.getString(2));

    }
    @Test
    public void batchTestStmt() throws SQLException {
        getConnection().createStatement().executeQuery("drop table if exists test_batch2");
        getConnection().createStatement().executeQuery("create table test_batch2 (id int not null primary key auto_increment, test varchar(10))");
        Statement stmt = getConnection().createStatement();
        stmt.addBatch("insert into test_batch2 values (null, 'hej1')");
        stmt.addBatch("insert into test_batch2 values (null, 'hej2')");
        stmt.addBatch("insert into test_batch2 values (null, 'hej3')");
        stmt.addBatch("insert into test_batch2 values (null, 'hej4')");
        stmt.executeBatch();
        ResultSet rs = getConnection().createStatement().executeQuery("select * from test_batch2");
        for(int i=1;i<=4;i++) {
            assertEquals(true,rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals("hej"+i,rs.getString(2));
        }
        assertEquals(false,rs.next());

    }
    @Test
    public void testChangeBatchHandler() throws SQLException {
        getConnection().createStatement().executeQuery("drop table if exists test_batch3");
        getConnection().createStatement().executeQuery("create table test_batch3 (id int not null primary key auto_increment, test varchar(10))");

        if(getConnection().isWrapperFor(DrizzleConnection.class)) {
            DrizzleConnection dc = getConnection().unwrap(DrizzleConnection.class);
            dc.setBatchQueryHandlerFactory(new NoopBatchHandlerFactory());
        }
        PreparedStatement ps = getConnection().prepareStatement("insert into test_batch3 (test) values (?)");
        PreparedStatement ps2 = getConnection().prepareStatement("insert into test_batch3 (test) values (?)");
        ps.setString(1,"From nr1");
        ps.addBatch();

        ps2.setString(1,"From nr2");
        ps2.addBatch();

        ps2.setString(1,"from nr2 2");
        ps.setString(1,"from nr1 2");
        ps.addBatch();
        ps2.addBatch();

        ps2.executeBatch();
        ps.executeBatch();



    }

    @Test
    public void floatingNumbersTest() throws SQLException {
        getConnection().createStatement().executeQuery("drop table if exists test_float");
        getConnection().createStatement().executeQuery("create table test_float (id int not null primary key auto_increment, a float )");

        PreparedStatement ps = getConnection().prepareStatement("insert into test_float (a) values (?)");
        ps.setDouble(1,3.99);
        ps.executeUpdate();
        ResultSet rs = getConnection().createStatement().executeQuery("select a from test_float");
        assertEquals(true,rs.next());
        assertEquals((float)3.99, rs.getFloat(1));
        assertEquals((float)3.99, rs.getFloat("a"));
        assertEquals(false,rs.next());
    }
    @Test
    public void dbmetaTest() throws SQLException {
        DatabaseMetaData dmd = getConnection().getMetaData();
        dmd.getBestRowIdentifier(null,"test_units_jdbc","t1",DatabaseMetaData.bestRowSession, true);
    }

    @Test
    public void manyColumnsTest() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeQuery("drop table if exists test_many_columns");
        String query = "create table test_many_columns (a0 int primary key not null";
        for(int i=1;i<1000;i++) {
            query+=",a"+i+" int";
        }
        query+=")";
        stmt.executeQuery(query);
        query="insert into test_many_columns values (0";
        for(int i=1;i<1000;i++) {
            query+=","+i;
        }
        query+=")";
        stmt.executeQuery(query);
        ResultSet rs = stmt.executeQuery("select * from test_many_columns");

        assertEquals(true,rs.next());
       
        for(int i=0;i<1000;i++) {
            assertEquals(rs.getInt("a"+i),i);
        }

    }

    @Test
    public void bigAutoIncTest() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeQuery("drop table if exists test_big_autoinc");
        stmt.executeQuery("create table test_big_autoinc (id int not null primary key auto_increment, test varchar(10))");
        stmt.executeQuery("alter table test_big_autoinc auto_increment = 1000");
        ResultSet rs = stmt.executeQuery("insert into test_big_autoinc values (null, 'hej')");
        ResultSet rsGen = stmt.getGeneratedKeys();
        assertEquals(true,rsGen.next());
        assertEquals(1000,rsGen.getInt(1));
        stmt.executeQuery("alter table test_big_autoinc auto_increment = "+Short.MAX_VALUE);
        stmt.executeQuery("insert into test_big_autoinc values (null, 'hej')");
        rsGen = stmt.getGeneratedKeys();
        assertEquals(true,rsGen.next());
        assertEquals(Short.MAX_VALUE,rsGen.getInt(1));
        stmt.executeQuery("alter table test_big_autoinc auto_increment = "+Integer.MAX_VALUE);
        stmt.executeQuery("insert into test_big_autoinc values (null, 'hej')");
        rsGen = stmt.getGeneratedKeys();
        assertEquals(true,rsGen.next());
        assertEquals(Integer.MAX_VALUE,rsGen.getInt(1));
    }

    @Test
    public void bigUpdateCountTest() throws SQLException {
        Statement stmt = getConnection().createStatement();
        stmt.executeQuery("drop table if exists test_big_update");
        stmt.executeQuery("create table test_big_update (id int primary key not null, updateme int)");
        for(int i=0;i<40000;i++) {
            stmt.executeQuery("insert into test_big_update values ("+i+","+i+")");
        }
        ResultSet rs = stmt.executeQuery("select count(*) from test_big_update");
        assertEquals(true,rs.next());
        assertEquals(40000,rs.getInt(1));
        int updateCount = stmt.executeUpdate("update test_big_update set updateme=updateme+1");
        assertEquals(40000,updateCount);
    }

    //@Test
    public void testBinlogDumping() throws SQLException {
        assertEquals(true, getConnection().isWrapperFor(ReplicationConnection.class));

        ReplicationConnection rc = getConnection().unwrap(ReplicationConnection.class);
        List<RawPacket> rpList = rc.startBinlogDump(891,"mysqld-bin.000001");
        for(RawPacket rp : rpList) {
            for(byte b:rp.getByteBuffer().array()) {
                System.out.printf("%x ",b);
            }
            System.out.printf("\n");
        }
    }
    
    @Test
    public void testCharacterStreams() throws SQLException, IOException {
        getConnection().createStatement().execute("drop table if exists streamtest");
        getConnection().createStatement().execute("create table streamtest (id int not null primary key, strm text)");
        PreparedStatement stmt = getConnection().prepareStatement("insert into streamtest (id, strm) values (?,?)");
        stmt.setInt(1,2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = getConnection().createStatement().executeQuery("select * from streamtest");
        rs.next();
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while((ch = rdr.read()) != -1) {
            sb.append((char)ch);
        }
        assertEquals(sb.toString(),(toInsert));
        rdr = rs.getCharacterStream(2);
        sb = new StringBuilder();
        
        while((ch = rdr.read()) != -1) {
            sb.append((char)ch);
        }
        assertEquals(sb.toString(),(toInsert));
        InputStream is = rs.getAsciiStream("strm");
        sb = new StringBuilder();

        while((ch = is.read()) != -1) {
            sb.append((char)ch);
        }
        assertEquals(sb.toString(),(toInsert));
        is = rs.getUnicodeStream("strm");
        sb = new StringBuilder();

        while((ch = is.read()) != -1) {
            sb.append((char)ch);
        }
        assertEquals(sb.toString(),(toInsert));
    }
    @Test
    public void testCharacterStreamWithLength() throws SQLException, IOException {
        getConnection().createStatement().execute("drop table if exists streamtest2");
        getConnection().createStatement().execute("create table streamtest2 (id int primary key not null, strm text)");
        PreparedStatement stmt = getConnection().prepareStatement("insert into streamtest2 (id, strm) values (?,?)");
        stmt.setInt(1,2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader, 5);
        stmt.execute();
        ResultSet rs = getConnection().createStatement().executeQuery("select * from streamtest2");
        rs.next();
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while((ch = rdr.read()) != -1) {
            sb.append((char)ch);
        }
        assertEquals(sb.toString(),toInsert.substring(0,5));
    }

    @Test
    public void testBlob() throws SQLException, IOException {
        getConnection().createStatement().execute("drop table if exists blobtest");
        getConnection().createStatement().execute("create table blobtest (id int not null primary key, strm blob)");
        PreparedStatement stmt = getConnection().prepareStatement("insert into blobtest (id, strm) values (?,?)");
        byte [] theBlob = {1,2,3,4,5,6};
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1,1);
        stmt.setBlob(2,stream);
        stmt.execute();
        ResultSet rs = getConnection().createStatement().executeQuery("select * from blobtest");
        rs.next();
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos=0;
        while((ch = readStuff.read())!=-1) {
            assertEquals(theBlob[pos++],ch);
        }

        readStuff = rs.getBinaryStream("strm");

        pos=0;
        while((ch = readStuff.read())!=-1) {
            assertEquals(theBlob[pos++],ch);
        }
    }
   @Test
    public void testBlobWithLength() throws SQLException, IOException {
        getConnection().createStatement().execute("drop table if exists blobtest");
        getConnection().createStatement().execute("create table blobtest (id int not null primary key, strm blob)");
        PreparedStatement stmt = getConnection().prepareStatement("insert into blobtest (id, strm) values (?,?)");
        byte [] theBlob = {1,2,3,4,5,6};
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1,1);
        stmt.setBlob(2,stream,4);
        stmt.execute();
        ResultSet rs = getConnection().createStatement().executeQuery("select * from blobtest");
        rs.next();
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos=0;
        while((ch = readStuff.read())!=-1) {
            assertEquals(theBlob[pos++],ch);
        }
    }
    @Test
    public void testEmptyResultSet() throws SQLException {
        getConnection().createStatement().execute("drop table if exists emptytest");
        getConnection().createStatement().execute("create table emptytest (id int)");
        Statement stmt = getConnection().createStatement();
        assertEquals(true,stmt.execute("SELECT * FROM emptytest"));
        assertEquals(false,stmt.getResultSet().next());
    }
    @Test
    public void testLongColName() throws SQLException {
        getConnection().createStatement().execute("drop table if exists longcol");
        DatabaseMetaData dbmd = getConnection().getMetaData();
        String str="";
        for(int i =0;i<dbmd.getMaxColumnNameLength();i++) {
            str+="x";   
        }
        getConnection().createStatement().execute("create table longcol ("+str+" int not null primary key)");
        getConnection().createStatement().execute("insert into longcol values (1)");
        ResultSet rs = getConnection().createStatement().executeQuery("select * from longcol");
        assertEquals(true,rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1,rs.getInt(str));
    }

    @Test(expected = SQLException.class)
    public void testBadParamlist() throws SQLException {
        PreparedStatement ps = getConnection().prepareStatement("insert into blah values (?)");
        ps.execute();
    }

    @Test
    public void setobjectTest() throws SQLException, IOException, ClassNotFoundException {
        getConnection().createStatement().execute("drop table if exists objecttest");
        getConnection().createStatement().execute(
                "create table objecttest (int_test int primary key not null, string_test varchar(30), timestamp_test timestamp, serial_test blob)");
        PreparedStatement ps = getConnection().prepareStatement("insert into objecttest values (?,?,?,?)");
        ps.setObject(1, 5);
        ps.setObject(2, "aaa");
        ps.setObject(3, Timestamp.valueOf("2009-01-17 15:41:01"));
        ps.setObject(4, new SerializableClass("testing",8));
        ps.execute();

        ResultSet rs = getConnection().createStatement().executeQuery("select * from objecttest");
        assertEquals(true,rs.next());
        Object theInt = rs.getObject(1);
        assertTrue(theInt instanceof Long);
        Object theInt2 = rs.getObject("int_test");
        assertTrue(theInt2 instanceof Long);
        Object theString = rs.getObject(2);
        assertTrue(theString instanceof String);
        Object theTimestamp = rs.getObject(3);
        assertTrue(theTimestamp instanceof Timestamp);
        Object theBlob = rs.getObject(4);

        byte [] rawBytes = rs.getBytes(4);
        ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        SerializableClass sc = (SerializableClass)ois.readObject();

        assertEquals(sc.getVal(), "testing");
        assertEquals(sc.getVal2(), 8);
        rawBytes = rs.getBytes("serial_test");
        bais = new ByteArrayInputStream(rawBytes);
        ois = new ObjectInputStream(bais);
        sc = (SerializableClass)ois.readObject();

        assertEquals(sc.getVal(), "testing");
        assertEquals(sc.getVal2(), 8);
    }
    @Test
    public void binTest() throws SQLException, IOException {
        getConnection().createStatement().execute("drop table if exists bintest");
        getConnection().createStatement().execute(
                "create table bintest (id int not null primary key auto_increment, bin1 varbinary(300), bin2 varbinary(300))");
        byte [] allBytes = new byte[256];
        for(int i=0;i<256;i++) {
            allBytes[i]=(byte) (i&0xff);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(allBytes);
        PreparedStatement ps = getConnection().prepareStatement("insert into bintest (bin1,bin2) values (?,?)");
        ps.setBytes(1,allBytes);
        ps.setBinaryStream(2, bais);
        ps.execute();

        ResultSet rs = getConnection().createStatement().executeQuery("select bin1,bin2 from bintest");
        assertTrue(rs.next());
        Blob blob = rs.getBlob(1);
        InputStream is = rs.getBinaryStream(1);

        for(int i=0;i<256;i++) {
            int read = is.read();
            assertEquals(i,read);
        }
        is = rs.getBinaryStream(2);

        for(int i=0;i<256;i++) {
            int read = is.read();
            assertEquals(i,read);
        }

    }
    @Test
    public void binTest2() throws SQLException, IOException {
        getConnection().createStatement().execute("drop table if exists bintest2");

        if(getConnection().getMetaData().getDatabaseProductName().toLowerCase().equals("mysql")) {
            getConnection().createStatement().execute(
                "create table bintest2 (bin1 longblob) engine=innodb");
        } else {
            getConnection().createStatement().execute(
                "create table bintest2 (id int not null primary key auto_increment, bin1 blob)");            
        }

        byte [] buf=new byte[1000000];
        for(int i=0;i<1000000;i++) {
            buf[i]=(byte)i;
        }
        InputStream is = new ByteArrayInputStream(buf);
        PreparedStatement ps = getConnection().prepareStatement("insert into bintest2 (bin1) values (?)");
        ps.setBinaryStream(1, is);
        ps.execute();
        ps = getConnection().prepareStatement("insert into bintest2 (bin1) values (?)");
        is = new ByteArrayInputStream(buf);
        ps.setBinaryStream(1, is);
        ps.execute();
        ResultSet rs = getConnection().createStatement().executeQuery("select bin1 from bintest2");
        assertEquals(true,rs.next());
        byte [] buf2 = rs.getBytes(1);
        for(int i=0;i<1000000;i++) {
            assertEquals((byte)i,buf2[i]);
        }

        assertEquals(true,rs.next());
        buf2 = rs.getBytes(1);
        for(int i=0;i<1000000;i++) {
            assertEquals((byte)i,buf2[i]);
        }
        assertEquals(false,rs.next());
    }
    @Test(expected=SQLIntegrityConstraintViolationException.class)
    public void testException1() throws SQLException {
        getConnection().createStatement().execute("drop table if exists extest");
        getConnection().createStatement().execute(
                "create table extest (id int not null primary key)");
        getConnection().createStatement().execute("insert into extest values (1)");
        getConnection().createStatement().execute("insert into extest values (1)");
    }

    @Test
    public void testExceptionDivByZero() throws SQLException {
        ResultSet rs = getConnection().createStatement().executeQuery("select 1/0");
        assertEquals(rs.next(),true);
        assertEquals(null, rs.getString(1));
        if(getConnection().getMetaData().getDatabaseProductName().toLowerCase().equals("drizzle")) {
            SQLWarning warning = rs.getWarnings();
            assertEquals("1 warning(s)",warning.getMessage());
        }
    }
    @Test(expected = SQLSyntaxErrorException.class)
    public void testSyntaxError() throws SQLException {
        getConnection().createStatement().executeQuery("create asdf b");
    }

    @Test
    public void testRewriteBatchHandler() throws SQLException {
        getConnection().createStatement().execute("drop table if exists rewritetest");
        getConnection().createStatement().execute(
                "create table rewritetest (id int not null primary key, a varchar(10), b int) engine=innodb");

        if(getConnection().isWrapperFor(DrizzleConnection.class)) {
            DrizzleConnection dc = getConnection().unwrap(DrizzleConnection.class);
            dc.setBatchQueryHandlerFactory(new RewriteParameterizedBatchHandlerFactory());
        }

        PreparedStatement ps = getConnection().prepareStatement("insert into rewritetest values (?,?,?)");
        for(int i = 0;i<10000;i++) {
            ps.setInt(1,i);
            ps.setString(2,"bbb"+i);
            ps.setInt(3,30+i);
            ps.addBatch();
        }
        ps.executeBatch();
        ResultSet rs = getConnection().createStatement().executeQuery("select * from rewritetest");
        int i = 0;
        while(rs.next()) {
            assertEquals(i++, rs.getInt("id"));
        }
        assertEquals(10000,i);
    }
    @Test
    public void testRewriteBatchHandlerWithDupKey() throws SQLException {
        getConnection().createStatement().execute("drop table if exists rewritetest2");
        getConnection().createStatement().execute(
                "create table rewritetest2 (id int not null primary key, a varchar(10), b int) engine=innodb");
                
        if(getConnection().isWrapperFor(DrizzleConnection.class)) {
            DrizzleConnection dc = getConnection().unwrap(DrizzleConnection.class);
            dc.setBatchQueryHandlerFactory(new RewriteParameterizedBatchHandlerFactory());
        }

        long startTime = System.currentTimeMillis();
        PreparedStatement ps = getConnection().prepareStatement("insert into rewritetest2 values (?,?,?) on duplicate key update a=values(a)");
        for(int i = 0;i<10000;i++) {
            ps.setInt(1,0);
            ps.setString(2,"bbb"+i);
            ps.setInt(3,30+i);
            ps.addBatch();
        }
        ps.executeBatch();
        System.out.println("time: "+(System.currentTimeMillis() - startTime));
        ResultSet rs = getConnection().createStatement().executeQuery("select * from rewritetest2");
        int i = 0;
        while(rs.next()) {
            assertEquals(i++, rs.getInt("id"));
        }
        assertEquals(1,i);
    }
    @Test
    public void testStripQueryWithComments() {
        String q = "select 'some query without comments'";
        String expectedQ = q;
        assertEquals(q, Utils.stripQuery(q));
        q = "select 1 /*this should be removed*/ from b";
        expectedQ = "select 1  from b";
        assertEquals(expectedQ,Utils.stripQuery(q));
        q = "select 1 /*this should be # removed*/ from b# crap";
        expectedQ = "select 1  from b";
        assertEquals(expectedQ,Utils.stripQuery(q));
        q = "select \"1 /*this should not be # removed*/\" from b# crap";
        expectedQ = "select \"1 /*this should not be # removed*/\" from b";
        assertEquals(expectedQ,Utils.stripQuery(q));
        q = "/**/select \"1 /*this should not be # removed*/\" from b# crap/**/";
        expectedQ = "select \"1 /*this should not be # removed*/\" from b";
        assertEquals(expectedQ,Utils.stripQuery(q));
        q = "/**/select '1 /*this should not be # removed*/' from b# crap/**/";
        expectedQ = "select '1 /*this should not be # removed*/' from b";
        assertEquals(expectedQ,Utils.stripQuery(q));
    }

    @Test
    public void testPreparedStatementsWithComments() throws SQLException {
        getConnection().createStatement().execute("drop table if exists commentPreparedStatements");
        getConnection().createStatement().execute(
                        "create table commentPreparedStatements (id int not null primary key auto_increment, a varchar(10))");

        String query = "INSERT INTO commentPreparedStatements (a) VALUES (?) # ?";
        PreparedStatement pstmt = getConnection().prepareStatement(query);
        pstmt.setString(1,"yeah");
        pstmt.execute();
    }
    @Test
    public void testPreparedStatementsWithQuotes() throws SQLException {
        getConnection().createStatement().execute("drop table if exists quotesPreparedStatements");
        getConnection().createStatement().execute(
                        "create table quotesPreparedStatements (id int not null primary key auto_increment, a varchar(10))");

        String query = "INSERT INTO quotesPreparedStatements (a) VALUES (\"hellooo?\") # ?";
        PreparedStatement pstmt = getConnection().prepareStatement(query);

        pstmt.execute();
    }

    @Test
    public void testCountChars() {
        assertEquals(1, Utils.countChars("?",'?'));
        assertEquals(2, Utils.countChars("??",'?'));
        assertEquals(1, Utils.countChars("?'?'",'?'));
        assertEquals(1, Utils.countChars("?\"?\"",'?'));
    }
    @Test
    public void bigDecimalTest() throws SQLException {
        BigDecimal bd = BigDecimal.TEN;
        getConnection().createStatement().execute("drop table if exists bigdectest");
        getConnection().createStatement().execute(
                        "create table bigdectest (id int not null primary key auto_increment, bd decimal) engine=innodb");
        PreparedStatement ps = getConnection().prepareStatement("insert into bigdectest (bd) values (?)");
        ps.setBigDecimal(1,bd);
        ps.executeQuery();

        ResultSet rs=getConnection().createStatement().executeQuery("select bd from bigdectest");
        assertTrue(rs.next());
        Object bb = rs.getObject(1);
        assertEquals(bd, bb);
        BigDecimal bigD = rs.getBigDecimal(1);
        BigDecimal bigD2 = rs.getBigDecimal("bd");
        assertEquals(bd,bigD);
        assertEquals(bd,bigD2);
        bigD = rs.getBigDecimal("bd");
        assertEquals(bd,bigD);
    }


   
    @Test
    public void byteTest() throws SQLException {
        getConnection().createStatement().execute("drop table if exists bytetest");
        getConnection().createStatement().execute(
                        "create table bytetest (id int not null primary key auto_increment, a int) engine=innodb");
        PreparedStatement ps = getConnection().prepareStatement("insert into bytetest (a) values (?)");
        ps.setByte(1,Byte.MAX_VALUE);
        ps.execute();
        ResultSet rs=getConnection().createStatement().executeQuery("select a from bytetest");
        assertTrue(rs.next());

        Byte bc = rs.getByte(1);
        Byte bc2 = rs.getByte("a");

        assertTrue(Byte.MAX_VALUE == bc);
        assertEquals(bc2, bc);


    }


    @Test
    public void shortTest() throws SQLException {
        getConnection().createStatement().execute("drop table if exists shorttest");
        getConnection().createStatement().execute(
                        "create table shorttest (id int not null primary key auto_increment,a int) engine=innodb");
        PreparedStatement ps = getConnection().prepareStatement("insert into shorttest (a) values (?)");
        ps.setShort(1,Short.MAX_VALUE);
        ps.execute();
        ResultSet rs=getConnection().createStatement().executeQuery("select a from shorttest");
        assertTrue(rs.next());

        Short bc = rs.getShort(1);
        Short bc2 = rs.getShort("a");

        assertTrue(Short.MAX_VALUE == bc);
        assertEquals(bc2, bc);


    }
   @Test
    public void doubleTest() throws SQLException {
        getConnection().createStatement().execute("drop table if exists doubletest");
        getConnection().createStatement().execute(
                        "create table doubletest (id int not null primary key auto_increment,a double) engine=innodb");
        PreparedStatement ps = getConnection().prepareStatement("insert into doubletest (a) values (?)");
        double d = 1.5;
        ps.setDouble(1,d);
        ps.execute();
        ResultSet rs=getConnection().createStatement().executeQuery("select a from doubletest");
        assertTrue(rs.next());
        Object b = rs.getObject(1);
        assertEquals(b.getClass(),Double.class);
        Double bc = rs.getDouble(1);
        Double bc2 = rs.getDouble("a");

        assertTrue(d == bc);
        assertEquals(bc2, bc);


    }


    @Test
    public void testResultSetPositions() throws SQLException {
        getConnection().createStatement().execute("drop table if exists ressetpos");
        getConnection().createStatement().execute(
                        "create table ressetpos (i int not null primary key) engine=innodb");
        getConnection().createStatement().execute("insert into ressetpos values (1),(2),(3),(4)");

        ResultSet rs =getConnection().createStatement().executeQuery("select * from ressetpos");
        assertTrue(rs.isBeforeFirst());
        rs.next();
        assertTrue(!rs.isBeforeFirst());
        assertTrue(rs.isFirst());
        rs.beforeFirst();
        assertTrue(rs.isBeforeFirst());
        while(rs.next());
        assertTrue(rs.isAfterLast());
        rs.absolute(4);
        assertTrue(!rs.isAfterLast());
        rs.absolute(2);
        assertEquals(2,rs.getInt(1));
        rs.relative(2);
        assertEquals(4,rs.getInt(1));
        assertTrue(!rs.next());
        rs.previous();
        assertEquals(4,rs.getInt(1));
        rs.relative(-3);
        assertEquals(1,rs.getInt(1));
        assertEquals(false,rs.relative(-1));
        assertEquals(1,rs.getInt(1));
        rs.last();
        assertEquals(4,rs.getInt(1));
        assertEquals(4,rs.getRow());
        assertTrue(rs.isLast());
        rs.first();
        assertEquals(1,rs.getInt(1));
        assertEquals(1,rs.getRow());
        rs.absolute(-1);
        assertEquals(4,rs.getRow());
        assertEquals(4,rs.getInt(1));
    }

    @Test(expected = SQLException.class)
    public void findColumnTest() throws SQLException {
        ResultSet rs = getConnection().createStatement().executeQuery("select 1 as 'hej'");
        assertEquals(1,rs.findColumn("hej"));

        rs.findColumn("nope");

    }
    @Test
    public void getStatementTest() throws SQLException {
        ResultSet rs = getConnection().createStatement().executeQuery("select 1 as 'hej'");
        Statement stmt = rs.getStatement();
    }
    @Test
    public void getUrlTest() throws SQLException {
        ResultSet rs = getConnection().createStatement().executeQuery("select 'http://drizzle.org' as url");
        rs.next();
        URL url = rs.getURL(1);
        assertEquals("http://drizzle.org",url.toString());
        url = rs.getURL("url");
        assertEquals("http://drizzle.org",url.toString());

    }
    @Test(expected = SQLException.class)
    public void getUrlFailTest() throws SQLException {
        ResultSet rs = getConnection().createStatement().executeQuery("select 'asdf' as url");
        rs.next();
        URL url = rs.getURL(1);
        

    }
    @Test(expected = SQLException.class)
    public void getUrlFailTest2() throws SQLException {
        ResultSet rs = getConnection().createStatement().executeQuery("select 'asdf' as url");
        rs.next();
        URL url = rs.getURL("url");


    }
    @Test
    public void setNull() throws SQLException {
        PreparedStatement ps = getConnection().prepareStatement("insert blabla (?)");
        ps.setString(1,null);
    }

    @Test
    public void testBug501452() throws SQLException {
        Connection conn = getConnection();
        if(conn.isWrapperFor(DrizzleConnection.class)) {
            DrizzleConnection dc = conn.unwrap(DrizzleConnection.class);
            dc.setBatchQueryHandlerFactory(new RewriteParameterizedBatchHandlerFactory());
        }
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("drop table if exists test_units_jdbc.bug501452");
        stmt.executeUpdate("CREATE TABLE test_units_jdbc.bug501452 (id int not null primary key, value varchar(20))");
        stmt.close();
        PreparedStatement ps=conn.prepareStatement("insert into bug501452 (id,value) values (?,?)");
        ps.setObject(1, 1);
        ps.setObject(2, "value for 1");
        ps.addBatch();

        ps.executeBatch();

        ps.setObject(1, 2);
        ps.setObject(2, "value for 2");
        ps.addBatch();

        ps.executeBatch();

        connection.commit();

    }

    @Test
    public void testBug525946() throws SQLException {
        Connection conn = getConnection();
        assertTrue(conn.getAutoCommit());
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
    }
    @Test
    public void testUpdateCount() throws SQLException {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("select 1") ;
        System.out.println(stmt.getUpdateCount());
    }

    @Test
    public void testSetObject() throws SQLException {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("drop table if exists test_units_jdbc.test_setobjectconv");
        stmt.executeUpdate("CREATE TABLE test_units_jdbc.test_setobjectconv (id int not null primary key auto_increment, v1 varchar(40), v2 varchar(40))");
        stmt.close();
        PreparedStatement ps = conn.prepareStatement("insert into test_setobjectconv values (null, ?, ?)");
        ps.setObject(1,"2009-01-01 00:00:00", Types.TIMESTAMP);
        ps.setObject(2, "33", Types.DOUBLE);
        ps.execute();
    }



}