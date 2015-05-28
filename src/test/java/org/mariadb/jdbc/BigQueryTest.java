package org.mariadb.jdbc;


import org.junit.Assume;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class BigQueryTest extends BaseTest{
    @Test
    public void sendBigQuery2() throws SQLException {

        Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigQuery2"));

        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists bigblob");
        stmt.execute("create table bigblob (id int not null primary key auto_increment, test longblob)");


        char [] arr = new char[20000000];
        for(int i = 0 ; i<arr.length; i++) {
            arr[i] = (char) ('a'+(i%10));
        }

        Statement s= connection.createStatement();
        StringBuilder query = new StringBuilder("INSERT INTO bigblob VALUES (null, '").
                append(arr).append("')");

        s.executeUpdate(query.toString());

        ResultSet rs = stmt.executeQuery("select * from bigblob");
        rs.next();
        byte [] newBytes = rs.getBytes(2);
        assertEquals(arr.length, newBytes.length);
        for(int i = 0; i<arr.length; i++) {
            assertEquals(arr[i], newBytes[i]);
        }
    }

    @Test
    public void sendBigPreparedQuery() throws SQLException {

    	Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigPreparedQuery"));

        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists bigblob2");
        stmt.execute("create table bigblob2 (id int not null primary key auto_increment, test longblob, test2 longblob)");

        byte [] arr = new byte[20000000];
        Arrays.fill(arr, (byte) 'a');
        byte [] arr2 = new byte[20000000];
        Arrays.fill(arr2, (byte) 'b');

        PreparedStatement ps = connection.prepareStatement("insert into bigblob2 values(null, ?,?)");
        ps.setBytes(1,arr);
        ps.setBytes(2,arr2);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select * from bigblob2");
        rs.next();
        byte [] newBytes = rs.getBytes(2);
        byte [] newBytes2 = rs.getBytes(3);
        assertEquals(arr.length, newBytes.length);
        assertEquals(arr2.length, newBytes2.length);
        for(int i = 0; i<arr.length; i++) {
            assertEquals(arr[i], newBytes[i]);
        }
        for(int i = 0; i<arr2.length; i++) {
            assertEquals(arr2[i], newBytes2[i]);
        }

    }
}
