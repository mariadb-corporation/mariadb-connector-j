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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;


public class DataTypeUnsignedTest extends BaseTest {


    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("unsignedBitTest", "id BIT(8)");
        createTable("unsignedTinyIntTest", "id TINYINT UNSIGNED");
        createTable("unsignedSmallIntTest", "id SMALLINT UNSIGNED");
        createTable("yearTest", "id YEAR(4) ");
        createTable("unsignedMediumIntTest", "id MEDIUMINT UNSIGNED");
        createTable("unsignedIntTest", "id INT UNSIGNED");
        createTable("unsignedBigIntTest", "id BIGINT UNSIGNED");
        createTable("unsignedDecimalTest", "id DECIMAL(65,20) UNSIGNED");
        createTable("unsignedFloatTest", "id FLOAT UNSIGNED");
        createTable("unsignedDoubleTest", "id DOUBLE UNSIGNED");
    }


    @Test
    public void unsignedBitTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into unsignedBitTest values (b'01000000')");
        sharedConnection.createStatement().execute("insert into unsignedBitTest values (b'00000001')");
        sharedConnection.createStatement().execute("insert into unsignedBitTest values (b'00000000')");
        sharedConnection.createStatement().execute("insert into unsignedBitTest values (null)");
        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedBitTest", false)) {
            unsignedBitTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedBitTest", true)) {
            unsignedBitTestResult(rs);
        }
    }

    private void unsignedBitTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            assertEquals(64, rs.getByte(1));
            if (rs.next()) {
                assertTrue(rs.getBoolean(1));
                assertEquals(1, rs.getByte(1));
                if (rs.next()) {
                    assertFalse(rs.getBoolean(1));
                    assertEquals(0, rs.getByte(1));
                    if (rs.next()) {
                        assertFalse(rs.getBoolean(1));
                        assertEquals(0, rs.getByte(1));
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


    @Test
    public void unsignedTinyIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into unsignedTinyIntTest values (200)");
        sharedConnection.createStatement().execute("insert into unsignedTinyIntTest values (120)");
        sharedConnection.createStatement().execute("insert into unsignedTinyIntTest values (1)");
        sharedConnection.createStatement().execute("insert into unsignedTinyIntTest values (null)");
        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedTinyIntTest", false)) {
            unsignedTinyIntTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedTinyIntTest", true)) {
            unsignedTinyIntTestResult(rs);
        }
    }

    private void unsignedTinyIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            assertEquals(200, rs.getShort(1));
            assertEquals(200, rs.getInt(1));
            assertEquals(200L, rs.getLong(1));
            assertEquals(200D, rs.getDouble(1), .000001);
            assertEquals(200F, rs.getFloat(1), .000001);
            assertEquals("200", rs.getString(1));
            assertEquals(new BigDecimal("200"), rs.getBigDecimal(1));
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
                    oneNullTest(rs);
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


    @Test
    public void unsignedSmallIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into unsignedSmallIntTest values (65535)");
        sharedConnection.createStatement().execute("insert into unsignedSmallIntTest values (32767)");
        sharedConnection.createStatement().execute("insert into unsignedSmallIntTest values (1)");
        sharedConnection.createStatement().execute("insert into unsignedSmallIntTest values (null)");

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedSmallIntTest", false)) {
            unsignedSmallIntTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedSmallIntTest", true)) {
            unsignedSmallIntTestResult(rs);
        }
    }

    private void unsignedSmallIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            assertEquals(65535, rs.getInt(1));
            assertEquals(65535L, rs.getLong(1));
            assertEquals(65535D, rs.getDouble(1), .000001);
            assertEquals(65535F, rs.getFloat(1), .000001);
            assertEquals("65535", rs.getString(1));
            assertEquals(new BigDecimal("65535"), rs.getBigDecimal(1));
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
                    oneNullTest(rs);
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

    @Test
    public void yearTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into yearTest values (2155)");
        sharedConnection.createStatement().execute("insert into yearTest values (0)");
        sharedConnection.createStatement().execute("insert into yearTest values (null)");

        try (Connection connection = setConnection("&yearIsDateType=false")) {
            try (ResultSet rs = DatatypeTest.getResultSet("select * from yearTest", false, connection)) {
                yearTestResult(rs, false);
            }

            try (ResultSet rs = DatatypeTest.getResultSet("select * from yearTest", true, connection)) {
                yearTestResult(rs, sharedOptions().useServerPrepStmts);
            }
        }
    }

    private void yearTestResult(ResultSet rs, boolean binary) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            assertEquals(2155, rs.getShort(1));
            assertEquals(2155, rs.getInt(1));
            assertEquals(2155L, rs.getLong(1));
            assertEquals(2155D, rs.getDouble(1), .000001);
            assertEquals(2155F, rs.getFloat(1), .000001);
            assertEquals("2155", rs.getString(1));
            assertEquals(new BigDecimal("2155"), rs.getBigDecimal(1));
            if (rs.next()) {
                assertEquals(0, rs.getByte(1));
                assertEquals(0, rs.getShort(1));
                assertEquals(0, rs.getInt(1));
                assertEquals(0, rs.getLong(1));
                assertEquals(0, rs.getDouble(1), .000001);
                assertEquals(0, rs.getFloat(1), .000001);
                assertEquals(new BigDecimal("0"), rs.getBigDecimal(1));
                assertEquals(binary ? "0" : "0000", rs.getString(1));
                if (rs.next()) {
                    nullTest(rs, false);
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

    @Test
    public void unsignedMediumIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into unsignedMediumIntTest values (16777215)");
        sharedConnection.createStatement().execute("insert into unsignedMediumIntTest values (8388607)");
        sharedConnection.createStatement().execute("insert into unsignedMediumIntTest values (1)");
        sharedConnection.createStatement().execute("insert into unsignedMediumIntTest values (null)");

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedMediumIntTest", false)) {
            unsignedMediumIntTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedMediumIntTest", true)) {
            unsignedMediumIntTestResult(rs);
        }
    }

    private void unsignedMediumIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            assertEquals(16777215, rs.getInt(1));
            assertEquals(16777215L, rs.getLong(1));
            assertEquals(16777215D, rs.getDouble(1), .000001);
            assertEquals(16777215F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("16777215"), rs.getBigDecimal(1));
            assertEquals("16777215", rs.getString(1));
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
                    oneNullTest(rs);
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

    @Test
    public void unsignedIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into unsignedIntTest values (4294967295)");
        sharedConnection.createStatement().execute("insert into unsignedIntTest values (2147483647)");
        sharedConnection.createStatement().execute("insert into unsignedIntTest values (1)");
        sharedConnection.createStatement().execute("insert into unsignedIntTest values (null)");
        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedIntTest", false)) {
            unsignedIntTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedIntTest", true)) {
            unsignedIntTestResult(rs);
        }
    }

    private void unsignedIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            assertEquals(4294967295L, rs.getLong(1));
            assertEquals(4294967295D, rs.getDouble(1), .000001);
            assertEquals(4294967295F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("4294967295"), rs.getBigDecimal(1));
            assertEquals("4294967295", rs.getString(1));
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
                    oneNullTest(rs);
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


    @Test
    public void unsignedBigIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into unsignedBigIntTest values (18446744073709551615)");
        sharedConnection.createStatement().execute("insert into unsignedBigIntTest values (9223372036854775807)");
        sharedConnection.createStatement().execute("insert into unsignedBigIntTest values (1)");
        sharedConnection.createStatement().execute("insert into unsignedBigIntTest values (null)");

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedBigIntTest", false)) {
            unsignedBigIntTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedBigIntTest", true)) {
            unsignedBigIntTestResult(rs);
        }
    }

    private void unsignedBigIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            longMustFail(rs);
            assertEquals(18446744073709551615F, rs.getFloat(1), .000001);
            assertEquals(18446744073709551615D, rs.getDouble(1), .000001);
            assertEquals(new BigDecimal("18446744073709551615"), rs.getBigDecimal(1));
            assertEquals("18446744073709551615", rs.getString(1));
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
                    oneNullTest(rs);
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


    @Test
    public void unsignedDecimalTest() throws SQLException {
        try (Statement statement = sharedConnection.createStatement()) {
            statement.execute("insert into unsignedDecimalTest values (123456789012345678901234567890.12345678901234567890)");
            statement.execute("insert into unsignedDecimalTest values (9223372036854775806)");
            statement.execute("insert into unsignedDecimalTest values (1.1)");
            statement.execute("insert into unsignedDecimalTest values (1.0)");
            statement.execute("insert into unsignedDecimalTest values (null)");
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedDecimalTest", false)) {
            unsignedDecimalTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedDecimalTest", true)) {
            unsignedDecimalTestResult(rs);
        }
    }

    private void unsignedDecimalTestResult(ResultSet rs) throws SQLException {
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
                        oneNullTest(rs, true, false);
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

    @Test
    public void unsignedFloatTest() throws SQLException {
        try (Statement statement = sharedConnection.createStatement()) {
            statement.execute("insert into unsignedFloatTest values (123456789012345678901234567890.12345678901234567890)");
            statement.execute("insert into unsignedFloatTest values (9223372036854775806)");
            statement.execute("insert into unsignedFloatTest values (1.1)");
            statement.execute("insert into unsignedFloatTest values (1.0)");
            statement.execute("insert into unsignedFloatTest values (null)");
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedFloatTest", false)) {
            unsignedFloatTestResult(rs, false);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedFloatTest", true)) {
            unsignedFloatTestResult(rs, sharedUsePrepare());
        }
    }


    private void unsignedFloatTestResult(ResultSet rs, boolean binary) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            longMustFail(rs);
            assertEquals(123456789012345678901234567890F, rs.getFloat(1), 2E25F);
            assertEquals(123456789012345678901234567890D, rs.getDouble(1), 2E25F);
            assertEquals(123456789012345678901234567890F, rs.getBigDecimal(1).floatValue(), 2E25F);
            assertEquals(binary ? "1.2345679E29" : "1.23457e29", rs.getString(1));
            if (rs.next()) {
                byteMustFail(rs);
                shortMustFail(rs);
                intMustFail(rs);
                //real data is 9223372036854775806.
                if (binary) {
                    assertEquals(9223372036854775807L, rs.getLong(1));
                } else {
                    assertEquals(9223369837831520256L, rs.getLong(1)); //text data bad precison
                }
                assertEquals(9223372036854775806F, rs.getFloat(1), 1E14F);
                assertEquals(9223372036854775806F, rs.getDouble(1), 1E14F);
                assertEquals(9223372036854775806F, rs.getBigDecimal(1).floatValue(), 1E14F);
                assertEquals(binary ? "9.223372E18" : "9.22337e18", rs.getString(1));
                if (rs.next()) {
                    assertEquals(1, rs.getByte(1));
                    assertEquals(1, rs.getShort(1));
                    assertEquals(1, rs.getInt(1));
                    assertEquals(1, rs.getLong(1));
                    assertEquals(1.1F, rs.getFloat(1), .000001);
                    assertEquals(1.1D, rs.getDouble(1), .000001);
                    assertEquals("1.1", rs.getString(1));
                    assertEquals(1.1f, rs.getBigDecimal(1).floatValue(), 1e-5f);
                    if (rs.next()) {
                        oneNullTest(rs, true, true);
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

    @Test
    public void unsignedDoubleTest() throws SQLException {
        try (Statement statement = sharedConnection.createStatement()) {
            statement.execute("insert into unsignedDoubleTest values (123456789012345678901234567890.12345678901234567890)");
            statement.execute("insert into unsignedDoubleTest values (9223372036854775806)");
            statement.execute("insert into unsignedDoubleTest values (1.1)");
            statement.execute("insert into unsignedDoubleTest values (1.0)");
            statement.execute("insert into unsignedDoubleTest values (null)");
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedDoubleTest", false)) {
            unsignedDoubleTestResult(rs);
        }

        try (ResultSet rs = DatatypeTest.getResultSet("select * from unsignedDoubleTest", true)) {
            unsignedDoubleTestResult(rs);
        }
    }

    private void unsignedDoubleTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            longMustFail(rs);
            assertEquals(1.2345678901234568e29F, rs.getFloat(1), 1);
            assertEquals(1.2345678901234568e29D, rs.getDouble(1), 1);
            assertEquals(new BigDecimal("1.2345678901234568e29"), rs.getBigDecimal(1));
            assertEquals("1.2345678901234568e29", rs.getString(1).toLowerCase());
            if (rs.next()) {
                byteMustFail(rs);
                shortMustFail(rs);
                intMustFail(rs);
                assertEquals(9223372036854775807L, rs.getLong(1)); //not 9223372036854775806 because of float precision
                assertEquals(9223372036854775806F, rs.getFloat(1), .000001);
                assertEquals(9223372036854775806D, rs.getDouble(1), .000001);
                assertEquals(new BigDecimal("9.223372036854776E+18"), rs.getBigDecimal(1));
                assertEquals("9.223372036854776e18", rs.getString(1).toLowerCase());
                if (rs.next()) {
                    assertEquals(1, rs.getByte(1));
                    assertEquals(1, rs.getShort(1));
                    assertEquals(1, rs.getInt(1));
                    assertEquals(1, rs.getLong(1));
                    assertEquals(1.1F, rs.getFloat(1), .000001);
                    assertEquals(1.1D, rs.getDouble(1), .000001);
                    assertEquals("1.1", rs.getString(1));
                    assertEquals(new BigDecimal("1.1"), rs.getBigDecimal(1));
                    if (rs.next()) {
                        oneNullTest(rs, true, true);
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

    private void oneNullTest(ResultSet rs) throws SQLException {
        oneNullTest(rs, false, false);
    }

    private void oneNullTest(ResultSet rs, boolean decimal, boolean floatingPoint) throws SQLException {
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
            nullTest(rs, decimal);
        } else {
            fail("must have result !");
        }
    }

    private void nullTest(ResultSet rs, boolean decimal) throws SQLException {
        if (!decimal) {
            assertFalse(rs.getBoolean(1));
        }
        assertEquals(0, rs.getByte(1));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getShort(1));
        assertEquals(0, rs.getInt(1));
        assertEquals(0, rs.getLong(1));
        assertEquals(0, rs.getDouble(1), .00001);
        assertEquals(0, rs.getFloat(1), .00001);
        assertNull(rs.getBigDecimal(1));
        assertNull(rs.getString(1));

    }

}
