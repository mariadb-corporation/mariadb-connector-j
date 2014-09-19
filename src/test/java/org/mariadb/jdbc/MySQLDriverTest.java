package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import javax.sql.rowset.CachedRowSet;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


public class MySQLDriverTest extends BaseTest {
    
    public MySQLDriverTest() throws SQLException {
        super();
    }
    @Test
    public void testAuthconnection() throws SQLException {
        requireMinimumVersion(5,0);
        Statement st = connection.createStatement();

        st.execute("grant all privileges on *.* to 'test'@'localhost' identified by 'test'");
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = openNewConnection(connU + "?user=test&password=test");
            stmt = conn.createStatement();
            stmt.executeUpdate("create table if not exists test_authconnection(i int)");
        } finally {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (conn != null)
                conn.close();
            st.execute("drop user 'test'@'localhost'");
            st.executeUpdate("drop table if exists test_authconnection");
            st.close();
        }
    }

    @Test
    public void testAuthConnectionProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user","test");
        props.setProperty("password","test");
        Statement st = connection.createStatement();
        st.execute("grant all privileges on *.* to 'test'@'localhost' identified by 'test'");
        Connection conn = openNewConnection(connU, props);
        conn.close();
    }
    
    @Test
    public void testBit() throws SQLException {
        connection.createStatement().execute("drop table if exists bittest");
        connection.createStatement().execute("create table bittest (a bit(1), b bit(3))");
        connection.createStatement().execute("insert into bittest values (null, null), (0, 0), (1, 1), (0, 2), (1, 3);");
        byte[] bytes = new byte[] {0, 0, 1, 0, 1};
        ResultSet rs = connection.createStatement().executeQuery("select * from bittest");
        int count = 0;
        while(rs.next()) {
            if(rs.getObject(1) != null) {
            	assertEquals(Boolean.class, rs.getObject(1).getClass());
            }
            assertEquals(bytes[count++], rs.getByte(1));
        }
    }
    @Test
    public void testSmallint() throws SQLException {

        connection.createStatement().execute("drop table if exists smallinttest");
        connection.createStatement().execute("create table smallinttest (i1 smallint, i2 smallint unsigned)");
        connection.createStatement().execute("insert into smallinttest values (null, null), (0, 0), (-1, 1), (-32768, 32767), (32767, 65535)");
        Integer[] ints = new Integer[] {null, 0, 1, 32767, 65535};
        ResultSet rs = connection.createStatement().executeQuery("select * from smallinttest");
        int count = 0;
        while(rs.next()) {
            assertEquals(ints[count++], rs.getObject(2));
            if(rs.getObject(2) != null) {
            	assertEquals(Integer.class, rs.getObject(2).getClass());
            }
        }
    }
    @Test
    public void testMediumint() throws SQLException {

        connection.createStatement().execute("drop table if exists mediuminttest");
        connection.createStatement().execute("create table mediuminttest (i1 mediumint, i2 mediumint unsigned)");
        connection.createStatement().execute("insert into mediuminttest values (null, null), (0, 0), (-1, 1), (-8388608, 8388607), (8388607, 16777215)");
        ResultSet rs = connection.createStatement().executeQuery("select * from mediuminttest");
        Integer[] ints = new Integer[] {null, 0, 1, 8388607, 16777215};
        int count = 0;
        while(rs.next()) {
            assertEquals(ints[count++], rs.getObject(2));
            if(rs.getObject(2) != null) {
            	assertEquals(Integer.class, rs.getObject(2).getClass());
            }
        }
    }
    @Test
    public void testTimestamp() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (t timestamp)");
        connection.createStatement().execute("insert into t values  ('1971-01-01 01:01:01'), ('2007-12-03 15:50:18'), ('2037-12-31 23:59:59')");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        String[] data = new String[] {"1971-01-01 01:01:01.0", "2007-12-03 15:50:18.0", "2037-12-31 23:59:59.0"};
        int count = 0;
        while(rs.next()) {
        	assertEquals(data[count++], rs.getTimestamp(1).toString());
        }
    }
    @Test
    public void testDatetime() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (t datetime)");
        connection.createStatement().execute("insert into t values (null), ('1000-01-01 00:00:00'), ('2007-12-03 15:47:32'), ('9999-12-31 23:59:59')");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        Timestamp[] data = new Timestamp[] {null, Timestamp.valueOf("1000-01-01 00:00:00"), Timestamp.valueOf("2007-12-03 15:47:32"), Timestamp.valueOf("9999-12-31 23:59:59")};
        int count = 0;
        while(rs.next()) {
        	assertEquals(data[count++], rs.getObject(1));
        }
    }
    @Test
    public void testFloat() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (f float)");
        connection.createStatement().execute("insert into t values (null), (-3.402823466E+38), (-1.175494351E-38), (0), (1.175494351E-38), (3.402823466E+38)");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        Float[] data = new Float[] {null, -3.40282E+38F, -1.17549E-38F, 0F, 1.17549E-38F, 3.40282E+38F};
        int count = 0;
        while(rs.next()) {
            assertEquals(data[count], rs.getObject(1));
            if(rs.getObject(1) != null) {
            	assertEquals(data[count].getClass(), rs.getObject(1).getClass());
            }
            count++;
        }
    }

    @Test
    public void testDouble() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (d double)");
        connection.createStatement().execute("insert into t values (null), (-1.7976931348623157E+308), (-2.2250738585072014E-308), (0), (2.2250738585072014E-308), (1.7976931348623157E+308)");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        Double[] data = new Double[] {null, -1.7976931348623157E+308, -2.2250738585072014E-308, 0D, 2.2250738585072014E-308, 1.7976931348623157E+308};
        int count = 0;
        while(rs.next()) {
            assertEquals(data[count], rs.getObject(1));
            if(rs.getObject(1) != null) {
            	assertEquals(data[count].getClass(), rs.getObject(1).getClass());
            }
            count++;
        }
    }
     @Test
    public void bigintTest() throws SQLException {
        connection.createStatement().execute("drop table if exists biginttest");
        connection.createStatement().execute(
                        "create table biginttest (i1 bigint, i2 bigint unsigned)");
        connection.createStatement().execute("insert into biginttest values (null, null), (0, 0), (-1, 1), (-9223372036854775808, 9223372036854775807), (9223372036854775807, 18446744073709551615)");
        ResultSet rs = connection.createStatement().executeQuery("select * from biginttest");
        assertTrue(rs.next());
        assertEquals(null, rs.getObject(1));
        assertEquals(null, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(Long.class, rs.getObject(1).getClass());
        assertEquals(BigInteger.class, rs.getObject(2).getClass());
        assertEquals((long) 0, rs.getObject(1));
        assertEquals(BigInteger.ZERO, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(-1l, rs.getObject(1));
        assertEquals(BigInteger.ONE, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(-9223372036854775808l, rs.getObject(1));
        assertEquals(new BigInteger("9223372036854775807"), rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(9223372036854775807l, rs.getObject(1));
        assertEquals(new BigInteger("18446744073709551615"), rs.getObject(2));
        assertFalse(rs.next());
    }

    @Test
    public void FiftyMBRow() throws SQLException  {
        ResultSet rs =   connection.createStatement().executeQuery("select @max_allowed_packet");
        rs.next();
        if (rs.getInt(1) < 50000000)
        {
            rs.close();
            return;
        }
        rs.close();

        rs = connection.createStatement().executeQuery("select repeat('a',50000000),1");
        assertTrue(rs.next());
        assertEquals(rs.getString(1).length(), 50000000);
        assertEquals(rs.getString(2).length(), 1);
        rs.close();
    }
    @Test
    // Test query with length around max  packet length. Requires max_allowed_packet to be >16M
    public void largeQueryWrite() throws SQLException {
        if (!checkMaxAllowedPacket("largeQueryWrite")) {
            return;
        }
        char[] str= new char[16*1024*1024];
        Arrays.fill(str, 'a');
        String prefix= "select length('";
        String suffix= "') as len";

        for (int i=16*1024*1024 - prefix.length()  -suffix.length() -5 ;
             i < 16*1024*1024 - prefix.length()  -suffix.length();
             i++) {
            String query = prefix;
            String val = new String(str,0, i);
            query += val;
            query += suffix;
            ResultSet rs = connection.createStatement().executeQuery(query);
            Assert.assertTrue(rs.next());
            assertEquals(rs.getInt(1), i);
        }
    }

    @Test
    public void largePreparedQueryWrite() throws SQLException {
        if (!checkMaxAllowedPacket("largePreparedQueryWrite"))
            return;

        char[] str= new char[16*1024*1024];
        Arrays.fill(str, 'a');
        String sql=  "select length(?) as len";

        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i=16*1024*1024 - sql.length() -5;
             i < 16*1024*1024 - sql.length();
             i++) {
            String val = new String(str,0, i);
            ps.setString(1,val);
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            assertEquals(rs.getInt(1), i);
            rs.close();
        }
    }

    @Test
    public void smallQueryWriteCompress() throws SQLException {
    	String oldParams = mParameters;
    	setParameters("&useCompression=true");
        String sql=  "select 1";
        ResultSet rs = connection.createStatement().executeQuery(sql);
        Assert.assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        rs.close();
        setParameters(oldParams);
    }
    @Test
    public void largePreparedQueryWriteCompress() throws SQLException {
        if(!checkMaxAllowedPacket("largePreparedQueryCompress"))
            return;
        setConnection("&useCompression=true");
        try {
            char[] str= new char[16*1024*1024];
            Arrays.fill(str, 'a');
            String sql=  "select ?";

            PreparedStatement ps = connection.prepareStatement(sql);
            for (int i=16*1024*1024 - sql.length() -5;
                 i < 16*1024*1024 - sql.length();
                 i++) {
                String val = new String(str,0, i);
                ps.setString(1,val);
                ResultSet rs = ps.executeQuery();
                Assert.assertTrue(rs.next());
                assertEquals(rs.getString(1).length(), i);
                rs.close();
                System.out.println("i=" +i);
            }
        } finally {
        }
    }

    /* Prepared statement metadata before/after executing the query */
    @Test
    public void preparedStatementMetadata() throws Exception{
        requireMinimumVersion(5,0);
        PreparedStatement ps = connection.prepareStatement("select * from information_schema.tables where 1=0");
        ResultSetMetaData m1 = ps.getMetaData();
        assertTrue(m1 != null);
        ResultSet rs1 = ps.executeQuery();
        ResultSetMetaData m2 = rs1.getMetaData();
        
        assertEquals(m1.getColumnCount(), m2.getColumnCount());
        for(int i=1; i <= m1.getColumnCount(); i++) {
            assertEquals(m1.getCatalogName(i), m2.getCatalogName(i));
            assertEquals(m1.getColumnClassName(i),m2.getColumnClassName(i));
            assertEquals(m1.getColumnDisplaySize(i),m2.getColumnDisplaySize(i));
            assertEquals(m1.getColumnLabel(i),m2.getColumnLabel(i));
            assertEquals(m1.getColumnName(i),m2.getColumnName(i));
            assertEquals(m1.getColumnType(i), m2.getColumnType(i));
            assertEquals(m1.getColumnTypeName(i),m2.getColumnTypeName(i));
        }
    }
    
     @Test
    public void preparedStatementMetadata2() throws Exception{
        requireMinimumVersion(5,0);
        PreparedStatement ps = connection.prepareStatement("select * from information_schema.tables where table_type=?");
        ResultSetMetaData m1 = ps.getMetaData();
        assertTrue(m1 != null);
        ps.setString(1, "");
        ResultSet rs1 = ps.executeQuery();
        ResultSetMetaData m2 = rs1.getMetaData();
        
        assertEquals(m1.getColumnCount(), m2.getColumnCount());
        for(int i=1; i <= m1.getColumnCount(); i++) {
            assertEquals(m1.getCatalogName(i), m2.getCatalogName(i));
            assertEquals(m1.getColumnClassName(i),m2.getColumnClassName(i));
            assertEquals(m1.getColumnDisplaySize(i),m2.getColumnDisplaySize(i));
            assertEquals(m1.getColumnLabel(i),m2.getColumnLabel(i));
            assertEquals(m1.getColumnName(i),m2.getColumnName(i));
            assertEquals(m1.getColumnType(i), m2.getColumnType(i));
            assertEquals(m1.getColumnTypeName(i),m2.getColumnTypeName(i));
        }
    }
    
    @Test
    public void conj40()  throws Exception{
      PreparedStatement ps = connection.prepareStatement("select ?");
      ps.setObject(1, new java.util.Date(), Types.TIMESTAMP);
    }
    @Test
    public void testWarnings() throws SQLException{
        Statement st= connection.createStatement();

        /* To throw warnings rather than errors, we need a non-strict sql_mode */
        st.execute("set sql_mode=''");
        st.execute("create table if not exists warnings_test(c char(2)) ");
        st.executeUpdate("insert into warnings_test values('123'),('124')");
        SQLWarning w = st.getWarnings();
        assertEquals(w.getMessage(),"Data truncated for column 'c' at row 1");
        assertEquals(w.getSQLState(),"01000");
        w = w.getNextWarning();
        assertEquals(w.getMessage(),"Data truncated for column 'c' at row 2");
        assertEquals(w.getSQLState(),"01000");

        assertEquals(w.getNextWarning(), null);
        st.clearWarnings();
        assertEquals(st.getWarnings(), null);
    }

    @Test
    public void testUpdateCount() throws SQLException {
        Statement st = connection.createStatement();
        st.execute("CREATE TABLE IF NOT EXISTS t_update_count(flag int, k varchar(10),name varchar(10))");
        try {
            int cnt = st.executeUpdate("INSERT into t_update_count values(1, '5', 'name1'), (1,'5','name2')");
            assertEquals(cnt,2);
            PreparedStatement ps = connection.prepareStatement("UPDATE t_update_count SET flag=0 WHERE (k='5' AND (name=? OR name=?))");
            ps.setString(1,"name1");
            ps.setString(2,"name2");
            cnt = ps.executeUpdate();
            assertEquals(cnt, 2);
            ResultSet rs = st.executeQuery("select count(*) from t_update_count where flag=0");
            rs.next();
            cnt = rs.getInt(1);
            assertEquals(cnt, 2);

        }
        finally {
            st.execute("DROP TABLE t_update_count");
        }
    }

    @Test
    public void LoadDataInfile() throws SQLException, IOException {
       // Create temp file.
        File temp = File.createTempFile("ldinfile", ".tmp");

        try {
            // Write to temp file
            BufferedWriter out = new BufferedWriter(new FileWriter(temp));
            for(int i=0; i < 10000; i++)
                out.write("aString\n");
            out.close();
            Statement st = connection.createStatement();
            st.execute("DROP TABLE IF EXISTS ldinfile");
            st.execute("CREATE TABLE  ldinfile(a varchar(10))");
            st.execute("LOAD DATA LOCAL INFILE '" +temp.getAbsolutePath().replace('\\','/') + "' INTO TABLE ldinfile");
            ResultSet rs = st.executeQuery("SELECT * FROM ldinfile");
            rs.next();
            assertEquals("aString", rs.getString(1));
            rs.close();
        } finally {
            temp.delete();
        }
    }

    @Test
    public void LoadDataInfileEmpty() throws SQLException, IOException {
       // Create temp file.
        File temp = File.createTempFile("ldinfile", ".tmp");

        try {
            Statement st = connection.createStatement();
            st.execute("DROP TABLE IF EXISTS ldinfile");
            st.execute("CREATE TABLE  ldinfile(a varchar(10))");
            st.execute("LOAD DATA LOCAL INFILE '" +temp.getAbsolutePath().replace('\\','/') + "' INTO TABLE ldinfile");
            ResultSet rs = st.executeQuery("SELECT * FROM ldinfile");
            assertFalse(rs.next());
            rs.close();
        } finally {
            temp.delete();
        }
    }
    @Test
    public void testBlob2() throws SQLException {
        byte[] bytes = new byte[]{(byte)0xff};
        PreparedStatement ps = connection.prepareStatement("select ?");
        ps.setBytes(1,bytes);
        ResultSet rs = ps.executeQuery();
        rs.next();
        byte[] result = rs.getBytes(1);
        assertEquals(result.length, 1);
        assertEquals(result[0], bytes[0]);
    }

    @Test
    public void TimestampWithMicroseconds() throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select '2001-01-01 11:11:11.123456'");
        rs.next();
        Timestamp ts = rs.getTimestamp(1);
        assertEquals(ts.getNanos(), 123456000);
    }

    @Test
    public void TimeWithMilliseconds() throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select '11:11:11.123'");
        rs.next();
        Time ts = rs.getTime(1);
        assertEquals(ts.getTime()%1000,123);
    }


    @Test
    public void preparedStatementTimestampWithMicroseconds() throws Exception {
        setConnection("&useFractionalSeconds=1");
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Timestamp ts = new Timestamp(sdf.parse("2001-01-01 11:11:11").getTime());
            ts.setNanos(123456000);

            PreparedStatement st = connection.prepareStatement("select ?");
            st.setTimestamp(1, ts);
            ResultSet rs = st.executeQuery();
            rs.next();
            Timestamp ts1 = rs.getTimestamp(1);
            assertEquals(ts1.getNanos(), 123456000);

            sdf = new SimpleDateFormat("HH:mm:ss");
            Time time = new Time(sdf.parse("11:11:11").getTime());

            time.setTime(time.getTime() + 111);


            st.setTime(1, time);
            rs = st.executeQuery();
            rs.next();
            Time time1 = rs.getTime(1);
            assertEquals(time1.getTime()%1000,111);
            assertEquals(time, time1);
        } finally {
        }
    }

    @Test
    public void preparedStatementParameterReuse() throws Exception {
        PreparedStatement ps = connection.prepareStatement("set @a=?,@b=?");
        ps.setString(1, "a");
        ps.setString(2, "b");
        ps.executeUpdate();

        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("select @a,@b");
        rs.next();
        assertEquals(rs.getString(1), "a");
        assertEquals(rs.getString(2), "b");
        rs.close();

        ps.setString(2,"c");
        ps.executeUpdate();

        rs = st.executeQuery("select @a,@b");
        rs.next();
        assertEquals(rs.getString(1), "a");
        assertEquals(rs.getString(2), "c");
        rs.close();

    }

    @Test
    public void UpdateCachedRowSet() throws Exception {
        Statement st = connection.createStatement();
        st.execute("DROP TABLE IF EXISTS updatable");
        st.execute("CREATE TABLE updatable(i int primary key, a varchar(10))");
        st.execute("INSERT INTO updatable values(1,'a')");
        st.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = st.executeQuery("SELECT * FROM updatable");
        CachedRowSet crs;
        try {
            /* Reference implementation of CachedRowSet might not always be available ? */
            crs =  (CachedRowSet)Class.forName("com.sun.rowset.CachedRowSetImpl").newInstance();
        } catch(ClassNotFoundException ex) {
            return;
        }
        crs.setType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        crs.setConcurrency(ResultSet.CONCUR_UPDATABLE);
        crs.populate(rs);
        assertTrue(crs.next());
        crs.updateString(2, "b");
        crs.updateRow();
        crs.acceptChanges(connection);
        crs.close();

        rs = st.executeQuery("SELECT * FROM updatable");
        rs.next();
        assertEquals("b", rs.getString(2));
    }

    @Test
    public void setMaxRowsTest() throws Exception {
        requireMinimumVersion(5,0);
        Statement st = connection.createStatement();
        assertEquals(0, st.getMaxRows());

        st.setMaxRows(3);
        assertEquals(3, st.getMaxRows());

        /* Check 3 rows are returned if maxRows is limited to 3 */
        ResultSet rs = st.executeQuery("select * from information_schema.tables");
        int cnt=0;

        while(rs.next()) {
            cnt++;
        }
        rs.close();
        assertEquals(3, cnt);

        /* Check that previous setMaxRows has no effect on following statements */
        Statement st2 = connection.createStatement();
        assertEquals(0, st2.getMaxRows());

        /* Check 3 rows are returned if maxRows is limited to 3 */
        ResultSet rs2 = st2.executeQuery("select * from information_schema.tables");
        cnt=0;
        while(rs2.next()) {
            cnt++;
        }
        rs2.close();
        assertTrue(cnt > 3);

        /* Check that attempt to use setMaxRows with negative limit fails */
        try {
           st.setMaxRows(-1);
           assertTrue("setMaxRows(-1) succeeded", false);
        } catch(SQLException e) {
           assertTrue(st.getMaxRows() == 3); /* limit should not change */
        }
    }
    
    @Test 
    public void sessionVariables() throws Exception {
    	setConnection("&sessionVariables=sql_mode=ANSI_QUOTES,collation_connection = utf8_bin");
    	ResultSet rs = connection.createStatement().executeQuery("select @@sql_mode,@@collation_connection");
    	rs.next();
    	assertEquals("ANSI_QUOTES",rs.getString(1));
    	assertEquals("utf8_bin",rs.getString(2));
    }
    
    @Test 
    public void executeStatementAfterConnectionClose() throws Exception {
        Statement st = connection.createStatement();
        connection.close();
        try {
            st.execute("select 1");        
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("closed connection"));
        }
    }

    @Test
     public void conj44() throws Exception {
       ResultSet rs = connection.prepareStatement("select '\\''").executeQuery();
       rs.next();
       assertEquals("'",rs.getString(1));
       rs = connection.prepareStatement("select '\\'a'").executeQuery();
       rs.next();
       assertEquals("'a",rs.getString(1));
    }
    
    @Test
    public void connectToDbWithDashInName() throws Exception {
    	String oldDb = mDatabase;
    	connection.createStatement().execute("create database `data-base`");
    	try {
    		setDatabase("data-base");
    		assertEquals("data-base",connection.getCatalog());
    	}
    	finally {
    		setDatabase(oldDb);
    	   connection.createStatement().execute("drop database `data-base`");
    	}
    }
    
    @Test
    public void connectCreateDB() throws Exception {
    	String oldParams = mParameters;
    	String oldDb = mDatabase;
    	setParameters("&createDB=true");
    	setDatabase("no-such-db");
    	try {
    		assertEquals("no-such-db",connection.getCatalog());
    	}
    	finally {
    		setParameters(oldParams);
    		setDatabase(oldDb);
    	   connection.createStatement().execute("drop database `no-such-db`");
    	}
    }
    
    
}
