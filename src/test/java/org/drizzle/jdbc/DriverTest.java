package org.drizzle.jdbc;

import org.junit.Test;
import org.junit.After;
import org.drizzle.jdbc.internal.drizzle.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.drizzle.packet.RawPacket;
import org.apache.log4j.BasicConfigurator;

import java.sql.*;
import java.util.List;
import java.io.*;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
public class DriverTest {
    public static String host = "localhost";
    private Connection connection;
    //static { BasicConfigurator.configure(); }

    public DriverTest() throws SQLException {
        try {
            Class.forName("org.drizzle.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("Could not load driver");
        }
        //connection = DriverManager.getConnection("jdbc:mysql:thin://aaa:bbb@"+host+":3306/test_units_jdbc");
        connection = DriverManager.getConnection("jdbc:drizzle://"+host+":4427/test_units_jdbc");

        Statement stmt = connection.createStatement();
        try { stmt.execute("drop table t1"); } catch (Exception e) {}
        stmt.execute("create table t1 (id int not null primary key auto_increment, test varchar(20))");
        stmt.execute("insert into t1 (test) values (\"hej1\")");
        stmt.execute("insert into t1 (test) values (\"hej2\")");
        stmt.execute("insert into t1 (test) values (\"hej3\")");
        stmt.execute("insert into t1 (test) values (null)");
    }
    @After
    public void close() throws SQLException {
        connection.close();
    }
    @Test
    public void doQuery() throws SQLException{
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        for(int i=1;i<4;i++) {
            rs.next();
            assertEquals(String.valueOf(i),rs.getString(1));
            assertEquals("hej"+i,rs.getString("test"));
        }
        rs.next();
        assertEquals("NULL",rs.getString("test"));
    }
    @Test(expected = SQLException.class)
    public void askForBadColumnTest() throws SQLException{
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.next();
        rs.getInt("non_existing_column");
    }
    @Test(expected = SQLException.class)
    public void askForBadColumnIndexTest() throws SQLException{
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t1");
        rs.next();
        rs.getInt(102);
    }

    @Test(expected = SQLException.class)
    public void badQuery() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeQuery("whraoaooa");
    }
    @Test
    public void shortOperations() {
        byte [] a = WriteBuffer.shortToByteArray((short) (99*256 + 77));

        assertEquals(a[0],77);
        assertEquals(a[1],99);
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
        PreparedStatement prepStmt = connection.prepareStatement(query);
        prepStmt.setString(1,"hej1");
        prepStmt.setInt(2,1);
        ResultSet results = prepStmt.executeQuery();
        String res = "";
        while(results.next()) {
            res=results.getString("test");
        }
        assertEquals("hej1",res);        
    }
    @Test
    public void updateTest() throws SQLException {
        String query = "UPDATE t1 SET test = ? where id = ?";
        PreparedStatement prepStmt = connection.prepareStatement(query);
        prepStmt.setString(1,"updated");
        prepStmt.setInt(2,3);
        int updateCount = prepStmt.executeUpdate();
        assertEquals(1,updateCount);
        String query2 = "SELECT * FROM t1 WHERE id=?";
        prepStmt = connection.prepareStatement(query2);
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
        Statement stmt = connection.createStatement();
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
    public void transactionTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeQuery("DROP TABLE IF EXISTS t3");
        stmt.executeQuery("CREATE TABLE t3 (id int not null primary key auto_increment, test varchar(20)) engine=innodb");
        connection.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO t3 (test) VALUES ('heja')");
        stmt.executeUpdate("INSERT INTO t3 (test) VALUES ('japp')");
        connection.commit();
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
        connection.rollback();
        rs = stmt.executeQuery("SELECT * FROM t3 WHERE id=3");
        assertEquals(false,rs.next());
    }
    @Test
    public void savepointTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("drop table if exists t4");
        stmt.executeUpdate("create table t4 (id int not null primary key auto_increment, test varchar(20)) engine=innodb");
        connection.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej1')");
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej2')");
        Savepoint savepoint = connection.setSavepoint("yep");
        stmt.executeUpdate("INSERT INTO t4 (test)  values('hej3')");
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej4')");
        connection.rollback(savepoint);
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej5')");
        stmt.executeUpdate("INSERT INTO t4 (test) values('hej6')");
        connection.commit();
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
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED,connection.getTransactionIsolation());
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,connection.getTransactionIsolation());
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE,connection.getTransactionIsolation());
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ,connection.getTransactionIsolation());
    }

    @Test
    public void isValidTest() throws SQLException {
        assertEquals(true,connection.isValid(0));
    }

    @Test
    public void connectionStringTest() throws SQLException {
        JDBCUrl url = new JDBCUrl("jdbc:drizzle://www.drizzle.org:4427/mmm");
        assertEquals("",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://www.drizzle.org:3306/mmm");
        assertEquals("",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = new JDBCUrl("jdbc:drizzle://whoa@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://whoa@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = new JDBCUrl("jdbc:drizzle://whoa:pass@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://whoa:pass@www.drizzle.org:4427/mmm");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("mmm",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = new JDBCUrl("jdbc:drizzle://whoa:pass@www.drizzle.org/aa");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("aa",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://whoa:pass@www.drizzle.org/aa");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("aa",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = new JDBCUrl("jdbc:drizzle://whoa:pass@www.drizzle.org/cc");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("cc",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://whoa:pass@www.drizzle.org/cc");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("cc",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = new JDBCUrl("jdbc:drizzle://whoa:pass@www.drizzle.org/bbb");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://whoa:pass@www.drizzle.org/bbb");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(3306,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.MYSQL, url.getDBType());

        url = new JDBCUrl("jdbc:drizzle://whoa:pass@www.drizzle.org/bbb/");
        assertEquals("whoa",url.getUsername());
        assertEquals("pass",url.getPassword());
        assertEquals("www.drizzle.org",url.getHostname());
        assertEquals(4427,url.getPort());
        assertEquals("bbb",url.getDatabase());
        assertEquals(JDBCUrl.DBType.DRIZZLE, url.getDBType());

        url = new JDBCUrl("jdbc:mysql:thin://whoa:pass@www.drizzle.org/bbb/");
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
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setString(1,"hej\"");
        ResultSet rs = stmt.executeQuery();
        assertEquals(false,rs.next());
    }

    @Test
    public void testPreparedWithNull() throws SQLException {
        String query = "insert into t1 (test) values (null)";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.execute();
        query = "select * from t1 where test is ?";
        pstmt = connection.prepareStatement(query);
        pstmt.setNull(1,1);
        ResultSet rs = pstmt.executeQuery();
        assertEquals(true,rs.next());
        assertEquals("NULL",rs.getString("test"));
        assertEquals(true,rs.wasNull());
    }

    @Test
    public void batchTest() throws SQLException {
        connection.createStatement().executeQuery("drop table if exists test_batch");
        connection.createStatement().executeQuery("create table test_batch (id int not null primary key auto_increment, test varchar(10))");

        PreparedStatement ps = connection.prepareStatement("insert into test_batch values (null, ?)");
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
        ResultSet rs = connection.createStatement().executeQuery("select * from test_batch");
        assertEquals(true,rs.next());
        assertEquals("aaa",rs.getString(2));
        assertEquals(true,rs.next());
        assertEquals("bbb",rs.getString(2));
        assertEquals(true,rs.next());
        assertEquals("ccc",rs.getString(2));

    }

    @Test
    public void floatingNumbersTest() throws SQLException {
        connection.createStatement().executeQuery("drop table if exists test_float");
        connection.createStatement().executeQuery("create table test_float (a float )");

        PreparedStatement ps = connection.prepareStatement("insert into test_float values (?)");
        ps.setDouble(1,3.99);
        ps.executeUpdate();
        ResultSet rs = connection.createStatement().executeQuery("select * from test_float");
        assertEquals(true,rs.next());
        assertEquals((float)3.99, rs.getFloat(1));
        assertEquals(false,rs.next());
    }
    @Test
    public void dbmetaTest() throws SQLException {
        DatabaseMetaData dmd = connection.getMetaData();
        dmd.getBestRowIdentifier(null,"test_units_jdbc","t1",DatabaseMetaData.bestRowSession, true);
    }

    @Test
    public void manyColumnsTest() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeQuery("drop table if exists test_many_columns");
        String query = "create table test_many_columns (a0 int";
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
        Statement stmt = connection.createStatement();
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
        Statement stmt = connection.createStatement();
        stmt.executeQuery("drop table if exists test_big_update");
        stmt.executeQuery("create table test_big_update (id int)");
        for(int i=0;i<40000;i++) {
            stmt.executeQuery("insert into test_big_update values ("+i+")");
        }
        ResultSet rs = stmt.executeQuery("select count(*) from test_big_update");
        assertEquals(true,rs.next());
        assertEquals(40000,rs.getInt(1));
        int updateCount = stmt.executeUpdate("update test_big_update set id=id+1");
        assertEquals(40000,updateCount);
    }

    //@Test
    public void testBinlogDumping() throws SQLException {
        assertEquals(true, connection.isWrapperFor(ReplicationConnection.class));

        ReplicationConnection rc = connection.unwrap(ReplicationConnection.class);
        List<RawPacket> rpList = rc.startBinlogDump(891,"mysqld-bin.000001");
        for(RawPacket rp : rpList) {
            for(byte b:rp.getRawBytes()) {
                System.out.printf("%x ",b);
            }
            System.out.printf("\n");
        }
    }
    
    @Test
    public void testCharacterStreams() throws SQLException, IOException {
        connection.createStatement().execute("drop table if exists streamtest");
        connection.createStatement().execute("create table streamtest (id int, strm text)");
        PreparedStatement stmt = connection.prepareStatement("insert into streamtest (id, strm) values (?,?)");
        stmt.setInt(1,2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader);
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery("select * from streamtest");
        rs.next();
        Reader rdr = rs.getCharacterStream("strm");
        StringBuilder sb = new StringBuilder();
        int ch;
        while((ch = rdr.read()) != -1) {
            sb.append((char)ch);
        }
        assertEquals(sb.toString(),(toInsert));
    }
    @Test
    public void testCharacterStreamWithLength() throws SQLException, IOException {
        connection.createStatement().execute("drop table if exists streamtest2");
        connection.createStatement().execute("create table streamtest2 (id int, strm text)");
        PreparedStatement stmt = connection.prepareStatement("insert into streamtest2 (id, strm) values (?,?)");
        stmt.setInt(1,2);
        String toInsert = "abcdefgh\njklmn\"";
        Reader reader = new StringReader(toInsert);
        stmt.setCharacterStream(2, reader, 5);
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery("select * from streamtest2");
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
        connection.createStatement().execute("drop table if exists blobtest");
        connection.createStatement().execute("create table blobtest (id int, strm blob)");
        PreparedStatement stmt = connection.prepareStatement("insert into blobtest (id, strm) values (?,?)");
        byte [] theBlob = {1,2,3,4,5,6};
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1,1);
        stmt.setBlob(2,stream);
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery("select * from blobtest");
        rs.next();
        InputStream readStuff = rs.getBlob("strm").getBinaryStream();
        int ch;
        int pos=0;
        while((ch = readStuff.read())!=-1) {
            assertEquals(theBlob[pos++],ch);
        }
    }
   @Test
    public void testBlobWithLength() throws SQLException, IOException {
        connection.createStatement().execute("drop table if exists blobtest");
        connection.createStatement().execute("create table blobtest (id int, strm blob)");
        PreparedStatement stmt = connection.prepareStatement("insert into blobtest (id, strm) values (?,?)");
        byte [] theBlob = {1,2,3,4,5,6};
        InputStream stream = new ByteArrayInputStream(theBlob);
        stmt.setInt(1,1);
        stmt.setBlob(2,stream,4);
        stmt.execute();
        ResultSet rs = connection.createStatement().executeQuery("select * from blobtest");
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
        connection.createStatement().execute("drop table if exists emptytest");
        connection.createStatement().execute("create table emptytest (id int)");
        Statement stmt = connection.createStatement();
        assertEquals(true,stmt.execute("SELECT * FROM emptytest"));
        assertEquals(false,stmt.getResultSet().next());
    }
}