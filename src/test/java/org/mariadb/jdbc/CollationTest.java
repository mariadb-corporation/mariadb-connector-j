/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.*;

import static org.junit.Assert.*;

public class CollationTest extends BaseTest {
    /**
     * Tables Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("emojiTest", "id int unsigned, field longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        createTable("unicodeTestChar", "id int unsigned, field1 varchar(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci, field2 longtext "
                + "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", "DEFAULT CHARSET=utf8mb4");
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
        try (Connection connection = setConnection()) {
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
            String serverCharset = rs.getString(1);
            if ("utf8mb4".equals(serverCharset)) mustThrowError = false;

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
            } catch (SQLException exception) {
                //mysql server thrown an HY000 state (not 22007), so a SQLException will be thrown, not a SQLDataException
                if (isMariadbServer()) assertTrue(exception instanceof SQLDataException);
                if (!mustThrowError) fail("Must not have thrown error");
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

    /**
     * CONJ-369 : Writes and reads a clob (longtext) of a latin1 table.
     *
     * @throws java.sql.SQLException if connection error occur.
     */
    @Test
    public void insertAndSelectShouldBothUseLatin1Encoding() throws SQLException {
        createTable("fooLatin1", "x longtext", "DEFAULT CHARSET=latin1");

        // German Umlaute (ÄÖÜ) U+00C4, U+00D6, U+00DC
        final String latin1String = "\u00c4\u00d6\u00dc";

        final Clob insertClob = sharedConnection.createClob();

        insertClob.setString(1, latin1String);

        final String insertSql = "INSERT INTO fooLatin1 VALUES(?)";
        PreparedStatement preparedStatement = sharedConnection.prepareStatement(insertSql);

        preparedStatement.setString(1, latin1String);
        preparedStatement.executeUpdate();

        preparedStatement.setClob(1, insertClob);
        preparedStatement.executeUpdate();

        final String selectSql = "select x from fooLatin1";
        ResultSet rs1 = preparedStatement.executeQuery(selectSql);

        assertTrue(rs1.next());
        assertEquals(latin1String, rs1.getString(1));

        assertTrue(rs1.next());
        assertEquals(latin1String, rs1.getString(1));
        Clob clob = rs1.getClob(1);
        assertEquals(latin1String, clob.getSubString(1, (int) clob.length()));

        assertFalse(rs1.next());
    }


}
