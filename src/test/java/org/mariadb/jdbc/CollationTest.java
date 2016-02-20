package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CollationTest extends BaseTest {
    /**
     * Tables Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("emojiTest", "id int unsigned, field longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        createTable("unicodeTestChar", "id int unsigned, field1 varchar(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, field2 longtext "
                + "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        createTable("textUtf8", "column1 text", "DEFAULT CHARSET=utf8");
        createTable("blobUtf8", "column1 blob", "DEFAULT CHARSET=utf8");
    }

    /**
     * Conj-92 and CONJ-118.
     *
     * @throws SQLException exception
     */
    @Test
    public void emoji() throws SQLException {
        Connection connection = null;
        try {
            connection = setConnection();
            String sqlForCharset = "select @@character_set_server";
            ResultSet rs = connection.createStatement().executeQuery(sqlForCharset);
            assertTrue(rs.next());
            final String serverCharacterSet = rs.getString(1);
            sqlForCharset = "select @@character_set_client";
            rs = connection.createStatement().executeQuery(sqlForCharset);
            assertTrue(rs.next());
            String clientCharacterSet = rs.getString(1);
            if ("utf8mb4".equalsIgnoreCase(serverCharacterSet)) {
                assertTrue(serverCharacterSet.equalsIgnoreCase(clientCharacterSet));
            } else {
                connection.createStatement().execute("SET NAMES utf8mb4");
            }
            PreparedStatement ps = connection.prepareStatement("INSERT INTO emojiTest (id, field) VALUES (1, ?)");
            byte[] emoji = new byte[]{(byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x84};
            ps.setBytes(1, emoji);
            ps.execute();
            ps = connection.prepareStatement("SELECT field FROM emojiTest");
            rs = ps.executeQuery();
            assertTrue(rs.next());
            // compare to the Java representation of UTF32
            assertEquals("\uD83D\uDE04", rs.getString(1));
        } finally {
            connection.close();
        }
    }


    /**
     * Conj-252.
     *
     * @throws SQLException exception
     */
    @Test
    public void test4BytesUtf8() throws Exception {

        String sqlForCharset = "select @@character_set_server";
        ResultSet rs = sharedConnection.createStatement().executeQuery(sqlForCharset);
        if (rs.next()) {
            String emoji = "\uD83C\uDF1F";
            boolean mustThrowError = true;
            if ("utf8mb4".equals(rs.getString(1))) {
                mustThrowError = false;
            }

            PreparedStatement ps = sharedConnection.prepareStatement("INSERT INTO unicodeTestChar (id, field1, field2) VALUES (1, ?, ?)");
            ps.setString(1, emoji);
            Reader reader = new StringReader(emoji);
            ps.setCharacterStream(2, reader);
            try {
                ps.execute();
                ps = sharedConnection.prepareStatement("SELECT field1, field2 FROM unicodeTestChar");
                rs = ps.executeQuery();
                assertTrue(rs.next());

                // compare to the Java representation of UTF32
                assertEquals(4, rs.getBytes(1).length);
                assertEquals(emoji, rs.getString(1));

                assertEquals(4, rs.getBytes(2).length);
                assertEquals(emoji, rs.getString(2));
            } catch (BatchUpdateException b) {
                if (!mustThrowError) {
                    fail("Must not have thrown error");
                }
            }
        } else {
            fail();
        }
    }

    @Test
    public void testText() throws SQLException {
        String str = "\u4f60\u597d(hello in Chinese)";
        try (PreparedStatement ps = sharedConnection.prepareStatement("insert into textUtf8 values (?)")) {
            ps.setString(1, str);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = sharedConnection.prepareStatement("select * from textUtf8");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String tmp = rs.getString(1);
                assertEquals(tmp, str);
            }
        }
    }

    @Test
    public void testBinary() throws SQLException {
        String str = "\u4f60\u597d(hello in Chinese)";
        byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
        try (PreparedStatement ps = sharedConnection.prepareStatement("insert into blobUtf8 values (?)")) {
            ps.setBytes(1, strBytes);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = sharedConnection.prepareStatement("select * from blobUtf8");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                byte[] tmp = rs.getBytes(1);
                for (int i = 0; i < tmp.length; i++) {
                    assertEquals(strBytes[i], tmp[i]);
                }

            }
        }
    }

}
