package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LocalInfileInputStreamTest extends BaseTest {

    @Test
    public void testLocalInfileInputStream() throws SQLException {
        Statement st = connection.createStatement();
        createTestTable("t","id int, test varchar(100)");

        // Build a tab-separated record file
        StringBuilder builder = new StringBuilder();
        builder.append("1\thello\n");
        builder.append("2\tworld\n");

        InputStream inputStream = new ByteArrayInputStream(builder.toString().getBytes());
        ((MySQLStatement) st).setLocalInfileInputStream(inputStream);

        st.executeUpdate("LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE t (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t");
        boolean next = rs.next();
        Assert.assertTrue(next);

        int count = rs.getInt(1);
        Assert.assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM t");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");

        st.close();
    }

    @Test
    public void testLocalInfile() throws SQLException {
        Statement st = connection.createStatement();
        createTestTable("t", "id int, test varchar(100)");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        st.executeUpdate("LOAD DATA LOCAL INFILE '"+classLoader.getResource("test.txt").getPath()+"' INTO TABLE t \n" +
                "  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n" +
                "  LINES TERMINATED BY '\\n' \n" +
                "  (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t");
        boolean next = rs.next();
        Assert.assertTrue(next);

        int count = rs.getInt(1);
        Assert.assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM t");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");

        st.close();
    }



    @Test(expected = SQLException.class)
    public void testPrepareLocalInfileWithoutInputStream() throws SQLException {
        PreparedStatement st = connection.prepareStatement("LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE t (id, test)");
        st.execute();
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
