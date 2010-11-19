package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.query.parameters.ByteParameter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Oct 18, 2010 Time: 8:22:07 PM To change this template use File |
 * Settings | File Templates.
 */
public class BigQueryTest {
    @Test
    public void sendBigQuery() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
        stmt.execute("drop table  if exists bigblob");
        stmt.execute("create table bigblob (id int not null primary key auto_increment, test longblob)");
        ResultSet rs = stmt.executeQuery("select @@max_allowed_packet");
        rs.next();
        int max_allowed_packet = rs.getInt(1);
        assertTrue("Max allowed packet on the server needs to be atleast 20m",max_allowed_packet > 0x00FFFFFF);
        char [] arr = new char[20000000];
        Arrays.fill(arr, 'a');

        Statement s= conn.createStatement();
        StringBuilder query = new StringBuilder("INSERT INTO bigblob VALUES (null, '").
                append(arr).append("')");
        //System.out.println(query.toString() );
        s.executeUpdate(query.toString());

       // rs = stmt.executeQuery("select * from bigblob");
                
       // rs.next();
       // assertEquals(new String(rs.getBytes(2)), new String(arr));
       // todo: bug , chunk it up when reading back data
    }

    @Test
    public void sendBigPreparedQuery() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://"+DriverTest.host+":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
        stmt.execute("drop table  if exists bigblob2");
        stmt.execute("create table bigblob2 (id int not null primary key auto_increment, test longblob, test2 longblob)");
        ResultSet rs = stmt.executeQuery("select @@max_allowed_packet");
        rs.next();
        int max_allowed_packet = rs.getInt(1);
        assertTrue("Max allowed packet on the server needs to be atleast 20m",max_allowed_packet > 0x00FFFFFF);
        byte [] arr = new byte[20000000];
        Arrays.fill(arr, (byte) 'a');
        byte [] arr2 = new byte[20000000];
        Arrays.fill(arr2, (byte) 'b');

        PreparedStatement ps = conn.prepareStatement("insert into bigblob2 values(null, ?,?)");
        ps.setBytes(1,arr);
        ps.setBytes(2,arr2);
        ps.executeUpdate();
       
    }
}
