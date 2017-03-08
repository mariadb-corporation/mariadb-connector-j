package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.sql.*;

import static org.junit.Assert.*;

public class LocalInfileInputStreamTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("LocalInfileInputStreamTest", "id int, test varchar(100)");
        createTable("ttlocal", "id int, test varchar(100)");
        createTable("ldinfile", "a varchar(10)");
        createTable("`infile`", "`a` varchar(50) DEFAULT NULL, `b` varchar(50) DEFAULT NULL",
                "ENGINE=InnoDB DEFAULT CHARSET=latin1");
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

        st.executeUpdate("LOAD DATA LOCAL INFILE 'dummy.tsv' INTO TABLE LocalInfileInputStreamTest (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM LocalInfileInputStreamTest");
        boolean next = rs.next();
        assertTrue(next);

        int count = rs.getInt(1);
        assertEquals(2, count);

        rs = st.executeQuery("SELECT * FROM LocalInfileInputStreamTest");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");

        st.close();
    }

    @Test
    public void testLocalInfileValidInterceptor() throws Exception {
        File temp = File.createTempFile("validateInfile", ".txt");
        StringBuilder builder = new StringBuilder();
        builder.append("1,hello\n");
        builder.append("2,world\n");
        BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
        bw.write(builder.toString());
        bw.close();
        testLocalInfile(temp.getAbsolutePath().replace("\\", "/"));
    }

    @Test
    public void testLocalInfileUnValidInterceptor() throws Exception {
        File temp = File.createTempFile("localInfile", ".txt");
        StringBuilder builder = new StringBuilder();
        builder.append("1,hello\n");
        builder.append("2,world\n");
        BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
        bw.write(builder.toString());
        bw.close();
        try {
            testLocalInfile(temp.getAbsolutePath().replace("\\", "/"));
            fail("Must have been intercepted");
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage().contains("LOCAL DATA LOCAL INFILE request to send local file named")
                    && sqle.getMessage().contains("not validated by interceptor \"org.mariadb.jdbc.LocalInfileInterceptorImpl\""));
        }
        //check that connection state is correct
        Statement st = sharedConnection.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1");
        rs.next();
        assertEquals(1, rs.getInt(1));
    }


    private void testLocalInfile(String file) throws SQLException {
        Statement st = sharedConnection.createStatement();
        st.executeUpdate("LOAD DATA LOCAL INFILE '" + file
                + "' INTO TABLE ttlocal "
                + "  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
                + "  (id, test)");

        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM ttlocal");
        boolean next = rs.next();

        assertTrue(next);
        assertEquals(2, rs.getInt(1));

        rs = st.executeQuery("SELECT * FROM ttlocal");

        validateRecord(rs, 1, "hello");
        validateRecord(rs, 2, "world");

        st.close();
    }

    @Test
    public void loadDataInfileEmpty() throws SQLException, IOException {
        // Create temp file.
        File temp = File.createTempFile("validateInfile", ".tmp");

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
            PreparedStatement st = sharedConnection.prepareStatement("LOAD DATA LOCAL INFILE 'validateInfile.tsv' "
                    + "INTO TABLE t (id, test)");
            st.execute();
            fail();
        } catch (SQLException e) {
            //check that connection is alright
            try {
                assertFalse(sharedConnection.isClosed());
                Statement st = sharedConnection.createStatement();
                st.execute("SELECT 1");
            } catch (SQLException eee) {
                fail();
            }
        }
    }

    private void validateRecord(ResultSet rs, int expectedId, String expectedTest) throws SQLException {
        boolean next = rs.next();
        assertTrue(next);

        int id = rs.getInt(1);
        String test = rs.getString(2);
        assertEquals(expectedId, id);
        assertEquals(expectedTest, test);
    }

    private File createTmpData(int recordNumber) throws Exception {
        File file = File.createTempFile("./infile" + recordNumber, ".tmp");

        //write it
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            // Every row is 8 bytes to make counting easier
            for (long i = 0; i < recordNumber; i++) {
                writer.write("\"a\",\"b\"");
                writer.write("\n");
            }
        }

        return file;
    }

    private void checkBigLocalInfile(int fileSize) throws Exception {
        int recordNumber = fileSize / 8;

        try (Statement statement = sharedConnection.createStatement()) {
            statement.execute("truncate `infile`");
            File file = createTmpData(recordNumber);

            try (InputStream is = new BufferedInputStream(new FileInputStream(file))) {
                MariaDbStatement stmt = statement.unwrap(MariaDbStatement.class);
                stmt.setLocalInfileInputStream(is);
                int insertNumber = stmt.executeUpdate("LOAD DATA LOCAL INFILE 'ignoredFileName' "
                            + "INTO TABLE `infile` "
                            + "COLUMNS TERMINATED BY ',' ENCLOSED BY '\\\"' ESCAPED BY '\\\\' "
                            + "LINES TERMINATED BY '\\n' (`a`, `b`)");
                assertEquals(insertNumber, recordNumber);
            }

            statement.setFetchSize(1000); //to avoid using too much memory for tests
            try (ResultSet rs = statement.executeQuery("SELECT * FROM `infile`")) {
                for (int i = 0; i < recordNumber; i++) {
                    assertTrue("record " + i + " doesn't exist",rs.next());
                    assertEquals("a", rs.getString(1));
                    assertEquals("b", rs.getString(2));
                }
                assertFalse(rs.next());
            }

        }
    }

    /**
     * CONJ-375 : error with local infile with size > 16mb.
     *
     * @throws Exception if error occus
     */
    @Test
    public void testSmallBigLocalInfileInputStream() throws Exception {
        checkBigLocalInfile(256);
    }

    @Test
    public void test2xBigLocalInfileInputStream() throws Exception {
        checkBigLocalInfile(16777216 * 2);
    }

    @Test
    public void test2xMaxAllowedPacketLocalInfileInputStream() throws Exception {
        ResultSet rs = sharedConnection.createStatement().executeQuery("select @@max_allowed_packet");
        rs.next();
        int maxAllowedPacket = rs.getInt(1);

        checkBigLocalInfile(maxAllowedPacket * 2);
    }

}
