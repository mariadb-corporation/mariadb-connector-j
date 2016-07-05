package org.mariadb.jdbc;


import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class BigQueryTest extends BaseTest {

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("bigblob", "id int not null primary key auto_increment, test longblob");
        createTable("bigblob2", "id int not null primary key auto_increment, test longblob, test2 longblob");
    }

    @Test
    public void sendBigQuery2() throws SQLException {

        Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigQuery2"));

        char[] arr = new char[20000000];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (char) ('a' + (i % 10));
        }

        Statement stmt = sharedConnection.createStatement();
        StringBuilder query = new StringBuilder("INSERT INTO bigblob VALUES (null, '")
                .append(arr).append("')");

        stmt.executeUpdate(query.toString());

        ResultSet rs = stmt.executeQuery("select * from bigblob");
        rs.next();
        byte[] newBytes = rs.getBytes(2);
        assertEquals(arr.length, newBytes.length);
        for (int i = 0; i < arr.length; i++) {
            assertEquals(arr[i], newBytes[i]);
        }
    }

    @Test
    public void sendBigPreparedQuery() throws SQLException {

        Assume.assumeTrue(checkMaxAllowedPacketMore40m("sendBigPreparedQuery"));


        byte[] arr = new byte[20000000];
        Arrays.fill(arr, (byte) 'a');
        byte[] arr2 = new byte[20000000];
        Arrays.fill(arr2, (byte) 'b');

        PreparedStatement ps = sharedConnection.prepareStatement("insert into bigblob2 values(null, ?,?)");
        ps.setBytes(1, arr);
        ps.setBytes(2, arr2);
        ps.executeUpdate();
        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from bigblob2");
        rs.next();
        byte[] newBytes = rs.getBytes(2);
        byte[] newBytes2 = rs.getBytes(3);
        assertEquals(arr.length, newBytes.length);
        assertEquals(arr2.length, newBytes2.length);
        for (int i = 0; i < arr.length; i++) {
            assertEquals(arr[i], newBytes[i]);
        }
        for (int i = 0; i < arr2.length; i++) {
            assertEquals(arr2[i], newBytes2[i]);
        }

    }


    @Test
    public void testError() throws SQLException {
        // check that maxAllowedPacket is big enough for the test
        Assume.assumeTrue(checkMaxAllowedPacketMore20m("testError"));

        Connection connection = null;
        try {
            connection = setConnection();
            int selectSize = 9;
            char[] arr = new char[16 * 1024 * 1024 - selectSize];
            Arrays.fill(arr, 'a');
            String request = "select '" + new String(arr) + "'";
            ResultSet rs = connection.createStatement().executeQuery(request);
            rs.next();
            assertEquals(arr.length, rs.getString(1).length());
        } finally {
            connection.close();
        }
    }


}
