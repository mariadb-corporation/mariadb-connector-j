package org.drizzle.jdbc;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.sql.Blob;
import java.sql.SQLException;
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

}
