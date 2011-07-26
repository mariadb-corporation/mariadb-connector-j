package org.skysql.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 13, 2009
 * Time: 1:29:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLDriverTest extends DriverTest {
    
    public MySQLDriverTest() throws SQLException {
        super();
    }
    @Test
    public void testAuthconnection() throws SQLException {

        Statement st = connection.createStatement();

        st.execute("grant all privileges on *.* to 'test'@'localhost' identified by 'test'");
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql:thin://test:test@localhost:3306/test&password=test");
            stmt = conn.createStatement();
            stmt.executeUpdate("create table if not exists test_authconnection(i int)");
        }
        finally {
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
        Connection conn = DriverManager.getConnection("jdbc:mysql:thin://teest:teest@localhost:3306/test",props);
        conn.close();
    }
    @Test
    public void testBit() throws SQLException {
        connection.createStatement().execute("drop table if exists bittest");
        connection.createStatement().execute("create table bittest (a bit(1), b bit(3))");
        connection.createStatement().execute("insert into bittest values (null, null), (0, 0), (1, 1), (0, 2), (1, 3);");
        ResultSet rs = connection.createStatement().executeQuery("select * from bittest");
        while(rs.next()) {
            if(rs.getObject(1) != null)
                System.out.println(rs.getObject(1).getClass());
            System.out.println(rs.getByte(1));
        }
    }
    @Test
    public void testSmallint() throws SQLException {

        connection.createStatement().execute("drop table if exists smallinttest");
        connection.createStatement().execute("create table smallinttest (i1 smallint, i2 smallint unsigned)");
        connection.createStatement().execute("insert into smallinttest values (null, null), (0, 0), (-1, 1), (-32768, 32767), (32767, 65535)");
        ResultSet rs = connection.createStatement().executeQuery("select * from smallinttest");
        while(rs.next()) {
            
            System.out.println(rs.getObject(2));
            if(rs.getObject(2) != null)
                System.out.println(rs.getObject(2).getClass());
            else System.out.println("---");
        }
    }
    @Test
    public void testMediumint() throws SQLException {

        connection.createStatement().execute("drop table if exists mediuminttest");
        connection.createStatement().execute("create table mediuminttest (i1 mediumint, i2 mediumint unsigned)");
        connection.createStatement().execute("insert into mediuminttest values (null, null), (0, 0), (-1, 1), (-8388608, 8388607), (8388607, 16777215)");
        ResultSet rs = connection.createStatement().executeQuery("select * from mediuminttest");
        while(rs.next()) {

            System.out.println(rs.getObject(2));
            if(rs.getObject(2) != null)
                System.out.println(rs.getObject(2).getClass());
            else System.out.println("---");
        }
    }
    @Test
    public void testTimestamp() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (t timestamp)");
        connection.createStatement().execute("insert into t values  ('1971-01-01 01:01:01'), ('2007-12-03 15:50:18'), ('2037-12-31 23:59:59')");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {
            System.out.println("---");
            System.out.println("ee "+rs.getTimestamp(1) );
         //   if(rs.getObject(1) != null)
           //     System.out.println("uu "+rs.getObject(1).getClass());
            //else System.out.println("xxx");
        }
    }
    @Test
    public void testDatetime() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (t datetime)");
        connection.createStatement().execute("insert into t values (null), ('1000-01-01 00:00:00'), ('2007-12-03 15:47:32'), ('9999-12-31 23:59:59')");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {
            System.out.println("---");
            System.out.println("ee "+rs.getObject(1));
         //   if(rs.getObject(1) != null)
           //     System.out.println("uu "+rs.getObject(1).getClass());
            //else System.out.println("xxx");
        }
    }
    @Test
    public void testFloat() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (f float)");
        connection.createStatement().execute("insert into t values (null), (-3.402823466E+38), (-1.175494351E-38), (0), (1.175494351E-38), (3.402823466E+38)");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {

            System.out.println(rs.getObject(1));
            if(rs.getObject(1) != null)
                System.out.println(rs.getObject(1).getClass());
            else System.out.println("---");
        }
    }

    @Test
    public void testDouble() throws SQLException {

        connection.createStatement().execute("drop table if exists t");
        connection.createStatement().execute("create table t (d double)");
        connection.createStatement().execute("insert into t values (null), (-1.7976931348623157E+308), (-2.2250738585072014E-308), (0), (2.2250738585072014E-308), (1.7976931348623157E+308)");
        ResultSet rs = connection.createStatement().executeQuery("select * from t");
        while(rs.next()) {

            System.out.println(rs.getObject(1));
            if(rs.getObject(1) != null)
                System.out.println(rs.getObject(1).getClass());
            else System.out.println("---");
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
        assertEquals(BigInteger.ZERO, rs.getObject(1));
        assertEquals(BigInteger.ZERO, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(new BigInteger("-1"), rs.getObject(1));
        assertEquals(BigInteger.ONE, rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(new BigInteger("-9223372036854775808"), rs.getObject(1));
        assertEquals(new BigInteger("9223372036854775807"), rs.getObject(2));
        assertTrue(rs.next());
        assertEquals(new BigInteger("9223372036854775807"), rs.getObject(1));
        assertEquals(new BigInteger("18446744073709551615"), rs.getObject(2));
        assertFalse(rs.next());
    }

    @Test
    // Test query with length around max  packet length. Requires max_allowed_packet to be >16M
    public void largeQueryWrite() throws SQLException {

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
        Connection conn = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/test?user=root&useCompression=true");
        try {

            String sql=  "select 1";


            ResultSet rs = conn.createStatement().executeQuery(sql);
            Assert.assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            rs.close();
        }
        finally {
            conn.close();
        }
    }
    @Test
    public void largePreparedQueryWriteCompress() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&useCompression=true");
        try {
            char[] str= new char[16*1024*1024];
            Arrays.fill(str, 'a');
            String sql=  "select ?";

            PreparedStatement ps = conn.prepareStatement(sql);
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
            conn.close();
        }
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
}
