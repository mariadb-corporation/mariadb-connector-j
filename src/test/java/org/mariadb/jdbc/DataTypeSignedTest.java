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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;


public class DataTypeSignedTest extends BaseTest {


    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("signedTinyIntTest", "id TINYINT");
        createTable("signedSmallIntTest", "id SMALLINT");
        createTable("signedMediumIntTest", "id MEDIUMINT");
        createTable("signedIntTest", "id INT");
        createTable("signedBigIntTest", "id BIGINT");
        createTable("signedDecimalTest", "id DECIMAL(65,20)");
    }

    @Test
    public void unsignedTinyIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (120)");
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (-1)");
        ResultSet rs = DatatypeTest.getResultSet("select * from signedTinyIntTest", false);
        assertTrue(signedTinyIntTestResult(rs));

        rs = DatatypeTest.getResultSet("select * from signedTinyIntTest", true);
        assertTrue(signedTinyIntTestResult(rs));
    }

    private boolean signedTinyIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            assertEquals(120, rs.getByte(1));
            assertEquals(120, rs.getShort(1));
            assertEquals(120, rs.getInt(1));
            assertEquals(120L, rs.getLong(1));
            assertEquals(120D, rs.getDouble(1), .000001);
            assertEquals(120F, rs.getFloat(1), .000001);
            assertEquals("120", rs.getString(1));
            assertEquals(new BigDecimal("120"), rs.getBigDecimal(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
                return true;
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
        return false;
    }


    @Test
    public void signedSmallIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (32767)");
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (-1)");

        ResultSet rs = DatatypeTest.getResultSet("select * from signedSmallIntTest", false);
        signedSmallIntTestResult(rs);

        rs = DatatypeTest.getResultSet("select * from signedSmallIntTest", true);
        signedSmallIntTestResult(rs);
    }

    private void signedSmallIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            assertEquals(32767, rs.getShort(1));
            assertEquals(32767, rs.getInt(1));
            assertEquals(32767L, rs.getLong(1));
            assertEquals(32767D, rs.getDouble(1), .000001);
            assertEquals(32767F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("32767"), rs.getBigDecimal(1));
            assertEquals("32767", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }


    @Test
    public void signedMediumIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (8388607)");
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (-1)");

        ResultSet rs = DatatypeTest.getResultSet("select * from signedMediumIntTest", false);
        signedMediumIntTestResult(rs);

        rs = DatatypeTest.getResultSet("select * from signedMediumIntTest", true);
        signedMediumIntTestResult(rs);
    }

    private void signedMediumIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            assertEquals(8388607, rs.getInt(1));
            assertEquals(8388607L, rs.getLong(1));
            assertEquals(8388607D, rs.getDouble(1), .000001);
            assertEquals(8388607F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("8388607"), rs.getBigDecimal(1));
            assertEquals("8388607", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void signedIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedIntTest values (2147483647)");
        sharedConnection.createStatement().execute("insert into signedIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedIntTest values (-1)");
        ResultSet rs = DatatypeTest.getResultSet("select * from signedIntTest", false);
        signedIntTestResult(rs);

        rs = DatatypeTest.getResultSet("select * from signedIntTest", true);
        signedIntTestResult(rs);
    }

    private void signedIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            assertEquals(2147483647, rs.getInt(1));
            assertEquals(2147483647L, rs.getLong(1));
            assertEquals(2147483647D, rs.getDouble(1), .000001);
            assertEquals(2147483647F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("2147483647"), rs.getBigDecimal(1));
            assertEquals("2147483647", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }


    @Test
    public void signedBigIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (9223372036854775807)");
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (-1)");

        ResultSet rs = DatatypeTest.getResultSet("select * from signedBigIntTest", false);
        signedBigIntTestResult(rs);

        rs = DatatypeTest.getResultSet("select * from signedBigIntTest", true);
        signedBigIntTestResult(rs);
    }

    private void signedBigIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            assertEquals(9223372036854775807L, rs.getLong(1));
            assertEquals(9223372036854775807F, rs.getFloat(1), .000001);
            assertEquals(9223372036854775807D, rs.getDouble(1), .000001);
            assertEquals(new BigDecimal("9223372036854775807"), rs.getBigDecimal(1));
            assertEquals("9223372036854775807", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }


    @Test
    public void signedDecimalTest() throws SQLException {
        Statement statement = sharedConnection.createStatement();
        statement.execute("insert into signedDecimalTest values (123456789012345678901234567890.12345678901234567890)");
        statement.execute("insert into signedDecimalTest values (9223372036854775806)");
        statement.execute("insert into signedDecimalTest values (1.1)");
        statement.execute("insert into signedDecimalTest values (1.0)");
        statement.execute("insert into signedDecimalTest values (null)");
        statement.execute("insert into signedDecimalTest values (-1)");

        ResultSet rs = DatatypeTest.getResultSet("select * from signedDecimalTest", false);
        signedDecimalTestResult(rs);

        rs = DatatypeTest.getResultSet("select * from signedDecimalTest", true);
        signedDecimalTestResult(rs);
    }

    private void signedDecimalTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            longMustFail(rs);
            assertEquals(123456789012345678901234567890.12345678901234567890F, rs.getFloat(1), 1000000000000000000000000D);
            assertEquals(123456789012345678901234567890.12345678901234567890F, rs.getFloat(1), 1000000000000000000000000D);
            assertEquals(123456789012345678901234567890.12345678901234567890D, rs.getDouble(1), 1000000000000000000000000D);
            assertEquals(new BigDecimal("123456789012345678901234567890.12345678901234567890"), rs.getBigDecimal(1));
            assertEquals("123456789012345678901234567890.12345678901234567890", rs.getString(1));
            if (rs.next()) {
                byteMustFail(rs);
                shortMustFail(rs);
                intMustFail(rs);
                assertEquals(9223372036854775806L, rs.getLong(1));
                assertEquals(9223372036854775806F, rs.getFloat(1), .000001);
                assertEquals(9223372036854775806D, rs.getDouble(1), .000001);
                assertEquals(new BigDecimal("9223372036854775806.00000000000000000000"), rs.getBigDecimal(1));
                assertEquals("9223372036854775806.00000000000000000000", rs.getString(1));
                if (rs.next()) {
                    assertEquals(1, rs.getByte(1));
                    assertEquals(1, rs.getShort(1));
                    assertEquals(1, rs.getInt(1));
                    assertEquals(1, rs.getLong(1));
                    assertEquals(1.1F, rs.getFloat(1), .000001);
                    assertEquals(1.1D, rs.getDouble(1), .000001);
                    assertEquals("1.10000000000000000000", rs.getString(1));
                    assertEquals(new BigDecimal("1.10000000000000000000"), rs.getBigDecimal(1));

                    if (rs.next()) {
                        oneNullNegativeTest(rs, true, false);
                    } else {
                        fail("must have result !");
                    }
                } else {
                    fail("must have result !");
                }
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }

    private void byteMustFail(ResultSet rs) {
        try {
            rs.getByte(1);
            fail("getByte must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void shortMustFail(ResultSet rs) {
        try {
            rs.getShort(1);
            fail("getShort must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void intMustFail(ResultSet rs) {
        try {
            rs.getInt(1);
            fail("getInt must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void longMustFail(ResultSet rs) {
        try {
            rs.getLong(1);
            fail("getLong must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void oneNullNegativeTest(ResultSet rs) throws SQLException {
        oneNullNegativeTest(rs, false, false);
    }

    private void oneNullNegativeTest(ResultSet rs, boolean decimal, boolean floatingPoint) throws SQLException {
        try {
            if (!decimal && !floatingPoint) {
                assertTrue(rs.getBoolean(1));
            }
            assertEquals(1, rs.getByte(1));
            assertEquals(1, rs.getShort(1));
            assertEquals(1, rs.getInt(1));
            assertEquals(1L, rs.getLong(1));
            assertEquals(1D, rs.getDouble(1), .000001);
            assertEquals(1F, rs.getFloat(1), .000001);
            if (decimal) {
                if (floatingPoint) {
                    BigDecimal bd = rs.getBigDecimal(1);
                    if (!bd.equals(new BigDecimal("1")) && !bd.equals(new BigDecimal("1.0"))) {
                        fail("getBigDecimal error : is " + bd.toString());
                    }
                    assertEquals("1.0", rs.getString(1));

                } else {
                    assertEquals(new BigDecimal("1.00000000000000000000"), rs.getBigDecimal(1));
                    assertEquals("1.00000000000000000000", rs.getString(1));

                }
            } else {
                assertEquals(new BigDecimal("1"), rs.getBigDecimal(1));
                assertEquals("1", rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("must not have thrown error");
        }

        if (rs.next()) {
            nullNegativeTest(rs, decimal);
        } else {
            fail("must have result !");
        }
    }

    private void nullNegativeTest(ResultSet rs, boolean decimal) throws SQLException {
        try {
            assertFalse(rs.getBoolean(1));
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getShort(1));
            assertEquals(0, rs.getInt(1));
            assertEquals(0, rs.getLong(1));
            assertEquals(0, rs.getDouble(1), .00001);
            assertEquals(0, rs.getFloat(1), .00001);
            assertNull(rs.getBigDecimal(1));
            assertNull(rs.getString(1));

        } catch (SQLException e) {
            e.printStackTrace();
            fail("must not have thrown error");
        }

        if (rs.next()) {
            try {
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.wasNull());
                assertEquals(-1, rs.getByte(1));
                assertEquals(-1, rs.getShort(1));
                assertEquals(-1, rs.getInt(1));
                assertEquals(-1, rs.getLong(1));
                assertEquals(-1, rs.getDouble(1), .00001);
                assertEquals(-1, rs.getFloat(1), .00001);
                if (decimal) {
                    assertTrue(new BigDecimal("-1.00000000000000000000").equals(rs.getBigDecimal(1)));
                    assertEquals("-1.00000000000000000000", rs.getString(1));
                } else {
                    assertTrue(new BigDecimal("-1").equals(rs.getBigDecimal(1)));
                    assertEquals("-1", rs.getString(1));
                }

            } catch (SQLException e) {
                e.printStackTrace();
                fail("must not have thrown error");
            }
        } else {
            fail("must have result !");
        }
    }

}
