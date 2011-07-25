package org.skysql.jdbc;

import org.junit.Ignore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Jan 14, 2009
 * Time: 7:58:11 AM
 */
@Ignore
public class BlobStreamingTest {
    public static String host = "localhost";
    private Connection connection;
    static { Logger.getLogger("").setLevel(Level.OFF); }

    public BlobStreamingTest() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/test");
       connection = DriverManager.getConnection("jdbc:drizzle://"+host+":3307/test?enableBlobStreaming=true");
       //connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test");
    }
  //  @After
    public void close() throws SQLException {
        connection.close();
    }
    public Connection getConnection() {
        return connection;
    }
    
   // @Test
    public void doQuery() throws SQLException, IOException {
        Statement stmt = getConnection().createStatement();
        stmt.execute("drop table  if exists bstreaming1");
        stmt.execute("create table bstreaming1 (id int not null primary key auto_increment, test longblob)");
        PreparedStatement ps = getConnection().prepareStatement("insert into bstreaming1 values (null, ?)");
        ByteArrayInputStream bais = new ByteArrayInputStream("HEJHEJHEJ".getBytes());
        ps.setBinaryStream(1, bais);
        ps.executeUpdate();
        
        stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery("select * from bstreaming1");
        assertEquals(rs.next(), true);
        byte[] b = new byte[100];
        int l = rs.getBinaryStream("test").read(b);
        assertEquals("HEJHEJHEJ",new String(b,0,l));
    }

}