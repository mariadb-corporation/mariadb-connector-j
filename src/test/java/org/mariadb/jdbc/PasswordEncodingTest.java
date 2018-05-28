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

import org.junit.Assume;
import org.junit.Test;

import java.nio.charset.Charset;
import java.sql.*;

import static org.junit.Assert.fail;

public class PasswordEncodingTest extends BaseTest {
    private static final String exoticPwd = "abéï你好";

    @Test
    public void testPwdCharset() throws Exception {
        //aurora user has no right to create other user
        Assume.assumeFalse("true".equals(System.getenv("AURORA")));
        Assume.assumeFalse(anonymousUser());

        String[] charsets = new String[]{"UTF-8",
                "windows-1252",
                "Big5"};
        String[] charsetsMysql = new String[]{"utf8",
                "latin1",
                "big5"};
        try {
            for (int i = 0; i < charsets.length; i++) {
                createUser(charsets[i], charsetsMysql[i]);
            }

            for (String currentCharsetName : charsets) {
                Connection connection = null;
                try {
                    connection = DriverManager.getConnection("jdbc:mariadb://" + ((hostname != null) ? hostname : "localhost")
                            + ":" + port + "/" + database + "?user=test" + currentCharsetName + "&password=" + exoticPwd);
                    //windows-1252 and windows-1250 will work have the same conversion for this password
                    if (!currentCharsetName.equals(Charset.defaultCharset().name())
                            && (!"windows-1252".equals(currentCharsetName) || !Charset.defaultCharset().name().startsWith("windows-125"))) {
                        fail("must have failed for currentCharsetName=" + currentCharsetName + " using java default charset "
                                + Charset.defaultCharset().name());
                    }
                } catch (SQLInvalidAuthorizationSpecException sqle) {
                    if (currentCharsetName.equals(Charset.defaultCharset().name())) {
                        fail("must have not have failed for charsetName=" + currentCharsetName + " which is java default");
                    }
                } finally {
                    if (connection != null) connection.close();
                }
            }

            for (String charsetName : charsets) checkConnection(charsetName, charsets);
        } finally {
            Statement stmt = sharedConnection.createStatement();
            for (String charsetName : charsets) {
                try {
                    stmt.execute("DROP USER 'test" + charsetName + "'@'%'");
                } catch (SQLException e) {
                    //nothing
                }
            }
        }


    }

    private void createUser(String charsetName, String serverCharset) throws Exception {
        Connection connection = null;
        try {
            connection = setConnection();
            MariaDbStatement stmt = connection.createStatement().unwrap(MariaDbStatement.class);
            stmt.execute("set @@character_set_client='" + serverCharset + "'");

            boolean useOldNotation = true;
            if ((isMariadbServer() && minVersion(10,2,0)) || (!isMariadbServer() && minVersion(8,0,0))) {
                useOldNotation = false;
            }
            if (useOldNotation) {
                stmt.execute("CREATE USER 'test" + charsetName + "'@'%'");
                stmt.testExecute("GRANT ALL on *.* to 'test" + charsetName + "'@'%' identified by '" + exoticPwd + "'", Charset.forName(charsetName));

            } else {
                stmt.testExecute("CREATE USER 'test" + charsetName + "'@'%' identified by '" + exoticPwd + "'", Charset.forName(charsetName));
                stmt.execute("GRANT ALL on *.* to 'test" + charsetName + "'@'%'");

            }
            stmt.execute("FLUSH PRIVILEGES");
        } finally {
            if (connection != null) connection.close();
        }
    }

    private void checkConnection(String charsetName, String[] charsets) throws Exception {

        for (String currentCharsetName : charsets) {
            Connection connection = null;
            try {
                connection = DriverManager.getConnection("jdbc:mariadb://" + ((hostname != null) ? hostname : "localhost")
                        + ":" + port + "/" + database + "?user=test" + charsetName + "&password="
                        + exoticPwd + "&passwordCharacterEncoding=" + currentCharsetName);
                if (!currentCharsetName.equals(charsetName)) {
                    fail("must have failed for charsetName=" + charsetName + " using passwordCharacterEncoding=" + currentCharsetName);
                }

            } catch (SQLInvalidAuthorizationSpecException sqle) {
                if (currentCharsetName.equals(charsetName)) {
                    fail("must not have failed for charsetName=" + charsetName + " using passwordCharacterEncoding=" + currentCharsetName);
                }
            } finally {
                if (connection != null) connection.close();
            }
        }
    }

}
