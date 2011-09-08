package org.skysql.jdbc;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BlobTest extends BaseTest {
    static { Logger.getLogger("").setLevel(Level.OFF); }
    @Test
    public void testPosition() throws SQLException {
        byte[] blobContent = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte [] pattern = new byte[]{3,4};
        Blob blob = new MySQLBlob(blobContent);
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
        Blob blob = new MySQLBlob(blobContent);
        blob.position(pattern,0);
    }
    @Test(expected = SQLException.class)
    public void testBadStart2() throws SQLException {
        byte[] blobContent = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        byte [] pattern = new byte[]{3,4};
        Blob blob = new MySQLBlob(blobContent);
        blob.position(pattern,44);
    }

    @Test
    public void testBug716378() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop table  if exists bug716378");
        stmt.execute("create table bug716378 (id int not null primary key auto_increment, test longblob, test2 blob, test3 text)");

        stmt.executeUpdate("insert into bug716378 values(null, 'a','b','c')");
        ResultSet rs = stmt.executeQuery("select * from bug716378");
        assertTrue(rs.next());
        assertEquals(MySQLBlob.class, rs.getObject(2).getClass());
        assertEquals(MySQLBlob.class, rs.getObject(3).getClass());
        assertEquals(MySQLClob.class, rs.getObject(4).getClass());
    }

    @Test
    public void blobSerialization() throws Exception {
       Blob b = new MySQLBlob(new byte[]{1,2,3});
       ByteArrayOutputStream baos = new ByteArrayOutputStream();
       ObjectOutputStream oos = new ObjectOutputStream(baos);
       oos.writeObject(b);

       ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
       MySQLBlob b2 = (MySQLBlob)ois.readObject();
       byte[] a = b2.getBytes(1, (int)b2.length());
       assertEquals(3, a.length);
       assertEquals(1, a[0]);
       assertEquals(2, a[1]);
       assertEquals(3, a[2]);
    }
}
