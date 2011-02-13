package org.drizzle.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: May 7, 2009
 * Time: 4:21:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class BlobTest {
    static { Logger.getLogger("").setLevel(Level.OFF); }
    @Test
    public void testPosition() throws SQLException {
        byte[] blobContent = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte [] pattern = new byte[]{3,4};
        Blob blob = new DrizzleBlob(blobContent);
        assertEquals(3, blob.position(pattern,1));
        pattern=new byte[]{12,13};
        assertEquals(-1, blob.position(pattern,1));
        pattern=new byte[]{11,12};
        assertEquals(11, blob.position(pattern,1));
        pattern=new byte[]{1,2};
        assertEquals(1, blob.position(pattern,1));

    }
    @Test(expected = SQLException.class)
    public void testBadStart() throws SQLException {
        byte[] blobContent = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte [] pattern = new byte[]{3,4};
        Blob blob = new DrizzleBlob(blobContent);
        blob.position(pattern,0);
    }
    @Test(expected = SQLException.class)
    public void testBadStart2() throws SQLException {
        byte[] blobContent = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte [] pattern = new byte[]{3,4};
        Blob blob = new DrizzleBlob(blobContent);
        blob.position(pattern,44);
    }

    @Test
    public void testBug716378() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:drizzle://" + DriverTest.host + ":3306/test_units_jdbc");
        Statement stmt = conn.createStatement();
        stmt.execute("drop table  if exists bug716378");
        stmt.execute("create table bug716378 (id int not null primary key auto_increment, test longblob, test2 blob, test3 text)");

        stmt.executeUpdate("insert into bug716378 values(null, 'a','b','c')");
        ResultSet rs = stmt.executeQuery("select * from bug716378");
        assertTrue(rs.next());
        assertEquals(DrizzleBlob.class, rs.getObject(2).getClass());
        assertEquals(DrizzleBlob.class, rs.getObject(3).getClass());
        assertEquals(String.class, rs.getObject(4).getClass());
    }

}
