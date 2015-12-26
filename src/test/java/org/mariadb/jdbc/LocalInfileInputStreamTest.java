package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;

public class LocalInfileInputStreamTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("t_local", "id int, test varchar(100)");
        createTable("tt_local", "id int, test varchar(100)");
        createTable("ldinfile", "a varchar(10)");
    }

    @Test
    public void testLocalInfileInputStream() throws SQLException {
        Statement st = sharedConnection.createStatement();


        // Build a tab-separated record file
        StringBuilder builder = new StringBuilder();
        builder.append("1\thello\n");
        builder.append("2\tworld\n");

        InputStream inputStream = new ByteArrayInputStream(builder.toString().getBytes());
        ((MariaDbStatement) st).setLocalInfileInputStream(inputStream);

        st.executeUpdate("LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE t_local (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t_local");
        boolean next = rs.next();
        Assert.assertTrue(next);

        int count = rs.getInt(1);
        Assert.assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM t_local");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");

        st.close();
    }

    @Test
    public void testLocalInfile() throws SQLException {
        Statement st = sharedConnection.createStatement();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ResultSet rs = st.executeQuery("select @@version_compile_os");
        if (!rs.next()) {
            return;
        }

        String os = rs.getString(1);
        if (os.toLowerCase().startsWith("win") || System.getProperty("os.name").startsWith("Windows")) {
            st.executeUpdate("LOAD DATA LOCAL INFILE '" + classLoader.getResource("test.txt").getPath()
                    + "' INTO TABLE tt_local "
                    + "  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
                    + "  LINES TERMINATED BY '\\r\\n' "
                    + "  (id, test)");
        } else {
            st.executeUpdate("LOAD DATA LOCAL INFILE '" + classLoader.getResource("test.txt").getPath()
                    + "' INTO TABLE tt_local "
                    + "  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
                    + "  LINES TERMINATED BY '\\n' "
                    + "  (id, test)");
        }


        rs = st.executeQuery("SELECT COUNT(*) FROM tt_local");
        boolean next = rs.next();

        Assert.assertTrue(next);

        int count = rs.getInt(1);
        Assert.assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM tt_local");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");

        st.close();
    }


    @Test
    public void loadDataInfileEmpty() throws SQLException, IOException {
        // Create temp file.
        File temp = File.createTempFile("ldinfile", ".tmp");

        try {
            Statement st = sharedConnection.createStatement();
            st.execute("LOAD DATA LOCAL INFILE '" + temp.getAbsolutePath().replace('\\', '/')
                    + "' INTO TABLE ldinfile");
            ResultSet rs = st.executeQuery("SELECT * FROM ldinfile");
            assertFalse(rs.next());
            rs.close();
        } finally {
            temp.delete();
        }
    }

    @Test
    public void testPrepareLocalInfileWithoutInputStream() throws SQLException {
        try {
            PreparedStatement st = sharedConnection.prepareStatement("LOAD DATA LOCAL INFILE 'dummy.tsv' "
                    + "INTO TABLE t (id, test)");
            st.execute();
            Assert.fail();
        } catch (SQLException e) {
            //check that connection is alright
            try {
                Assert.assertFalse(sharedConnection.isClosed());
                Statement st = sharedConnection.createStatement();
                st.execute("SELECT 1");
            } catch (SQLException eee) {
                Assert.fail();
            }
        }
    }

    private void validateRecord(ResultSet rs, int expectedId, String expectedTest) throws SQLException {
        boolean next = rs.next();
        Assert.assertTrue(next);

        int id = rs.getInt(1);
        String test = rs.getString(2);
        Assert.assertEquals(expectedId, id);
        Assert.assertEquals(expectedTest, test);
    }
}
