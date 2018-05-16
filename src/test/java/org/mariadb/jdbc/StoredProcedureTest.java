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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;
import org.junit.Ignore;

public class StoredProcedureTest extends BaseTest {
    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("useParameterName", "(a int) begin select a; end");
        createProcedure("useWrongParameterName", "(a int) begin select a; end");
        createProcedure("multiResultSets", "() BEGIN  SELECT 1; SELECT 2; END");
        createProcedure("inoutParam", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("testGetProcedures", "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");
        createProcedure("withStrangeParameter", "(IN a DECIMAL(10,2)) begin select a; end");
        createProcedure("TEST_SP1", "() BEGIN\n"
                + "SELECT @Something := 'Something';\n"
                + "SIGNAL SQLSTATE '70100'\n"
                + "SET MESSAGE_TEXT = 'Test error from SP'; \n"
                + "END");

        //sequence table are not in MySQL and MariaDB < 10.1, so create some basic table
        createTable("table_10", "val int");
        createTable("table_5", "val int");
        if (testSingleHost) {
            try (Statement stmt = sharedConnection.createStatement()) {
                stmt.execute("INSERT INTO table_10 VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
                stmt.execute("INSERT INTO table_5 VALUES (1),(2),(3),(4),(5)");
            }
        }
    }


    @Test
    public void testStoreProcedureStreaming() throws Exception {
        // aurora doesn't send back output results parameter when having SELECT results, even with flag enabled
        Assume.assumeFalse(sharedIsAurora());

        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("StoredWithOutput", "(out MAX_PARAM TINYINT, out MIN_PARAM TINYINT, out NULL_PARAM TINYINT)"
                + "begin select 1,0,null into MAX_PARAM, MIN_PARAM, NULL_PARAM from dual; SELECT * from table_10; SELECT * from table_5;end");

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{call StoredWithOutput(?,?,?)}")) {
            //indicate to stream results
            callableStatement.setFetchSize(1);

            callableStatement.registerOutParameter(1, Types.BIT);
            callableStatement.registerOutParameter(2, Types.BIT);
            callableStatement.registerOutParameter(3, Types.BIT);
            callableStatement.execute();


            ResultSet rs = callableStatement.getResultSet();
            for (int i = 1; i <= 10; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            //force reading of all result-set since output parameter are in the end.
            assertEquals(true, callableStatement.getBoolean(1));
            assertEquals(false, callableStatement.getBoolean(2));
            assertEquals(false, callableStatement.getBoolean(3));

            assertTrue(callableStatement.getMoreResults());

            rs = callableStatement.getResultSet();
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }

    }

    @Test
    public void testStoreProcedureStreamingWithAnotherQuery() throws Exception {
        // aurora doesn't send back output results parameter when having SELECT results, even with flag enabled
        Assume.assumeFalse(sharedIsAurora());

        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("StreamInterrupted", "(out MAX_PARAM TINYINT, out MIN_PARAM TINYINT, out NULL_PARAM TINYINT)"
                + "begin select 1,0,null into MAX_PARAM, MIN_PARAM, NULL_PARAM from dual; SELECT * from table_10; SELECT * from table_5;end");

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{call StreamInterrupted(?,?,?)}")) {
            //indicate to stream results
            callableStatement.setFetchSize(1);

            callableStatement.registerOutParameter(1, Types.BIT);
            callableStatement.registerOutParameter(2, Types.BIT);
            callableStatement.registerOutParameter(3, Types.BIT);

            callableStatement.execute();


            ResultSet rs = callableStatement.getResultSet();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));


            //execute another query on same connection must force loading of
            //existing streaming result-set
            try (Statement stmt = sharedConnection.createStatement()) {
                ResultSet otherRs = stmt.executeQuery("SELECT 'test'");
                assertTrue(otherRs.next());
                assertEquals("test", otherRs.getString(1));
            }

            for (int i = 2; i <= 10; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            assertEquals(true, callableStatement.getBoolean(1));
            assertEquals(false, callableStatement.getBoolean(2));
            assertEquals(false, callableStatement.getBoolean(3));

            //force reading of all result-set since output parameter are in the end.
            assertTrue(callableStatement.getMoreResults());

            rs = callableStatement.getResultSet();
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }

    }

    @Test
    public void testStoreProcedureStreamingWithoutOutput() throws Exception {
        createProcedure("StreamWithoutOutput", "(IN MAX_PARAM TINYINT)"
                + "begin SELECT * from table_10; SELECT * from table_5;end");

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{call StreamWithoutOutput(?)}")) {
            //indicate to stream results
            callableStatement.setFetchSize(1);
            callableStatement.setInt(1, 100);
            callableStatement.execute();

            ResultSet rs = callableStatement.getResultSet();
            for (int i = 1; i <= 10; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());

            assertTrue(callableStatement.getMoreResults());

            rs = callableStatement.getResultSet();
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }

    }

    @Before
    public void checkSp() throws SQLException {
        requireMinimumVersion(5, 0);
    }

    @Test
    public void callSimple() throws SQLException {
        CallableStatement st = sharedConnection.prepareCall("{? = call pow(?,?)}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        int result = st.getInt(1);
        assertEquals(result, 4);
    }
    
    @Test @Ignore
    public void callSimpleWithNewlines() throws SQLException {
        // Violates JDBC spec, but MySQL Connector/J allows it
        CallableStatement st = sharedConnection.prepareCall("{\r\n ? =  call pow(?,  ?  )   }");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        int result = st.getInt(1);
        assertEquals(result, 4);
        
        st = sharedConnection.prepareCall("{\n ? = call pow(?, ?)}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        result = st.getInt(1);
        assertEquals(result, 4);
        
        st = sharedConnection.prepareCall("{? = call pow  (\n?, ?  )}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        result = st.getInt(1);
        assertEquals(result, 4);
        
        st = sharedConnection.prepareCall("\r\n{\r\n?\r\n=\r\ncall\r\npow\r\n(\n?,\r\n?\r\n)\r\n}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        result = st.getInt(1);
        assertEquals(result, 4);
    }

    @Test
    public void callWithOutParameter() throws SQLException {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("prepareStmtWithOutParameter", "(x int, INOUT y int)\n"
                + "BEGIN\n"
                + "SELECT 1;end\n");
        CallableStatement callableStatement = sharedConnection.prepareCall("{call prepareStmtWithOutParameter(?,?)}");
        callableStatement.registerOutParameter(2, Types.INTEGER);
        callableStatement.setInt(1, 2);
        callableStatement.setInt(2, 3);
        callableStatement.execute();
        assertEquals(3, callableStatement.getInt(2));
    }

    @Test
    public void callWithResultSet() throws Exception {
        createProcedure("withResultSet", "(a int) begin select a; end");
        CallableStatement stmt = sharedConnection.prepareCall("{call withResultSet(?)}");
        stmt.setInt(1, 1);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        int res = rs.getInt(1);
        assertEquals(res, 1);
    }

    @Test
    public void callUseParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("a", 1);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        int res = rs.getInt(1);
        assertEquals(res, 1);
    }

    @Test(expected = SQLException.class)
    public void callUseWrongParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("b", 1);
        fail("must fail");
    }


    @Test
    public void callMultiResultSets() throws Exception {
        executeAndCheckResult(sharedConnection.prepareCall("{call multiResultSets()}"));
    }

    @Test
    public void prepareMultiResultSets() throws Exception {
        executeAndCheckResult(sharedConnection.prepareStatement("{call multiResultSets()}"));
    }

    private void executeAndCheckResult(PreparedStatement stmt) throws Exception {
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
    }


    @Test
    public void callInoutParam() throws SQLException {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        CallableStatement storedProc = sharedConnection.prepareCall("{call inOutParam(?)}");
        storedProc.registerOutParameter(1, Types.INTEGER);
        storedProc.setInt(1, 1);
        storedProc.execute();
        assertEquals(2, storedProc.getObject(1));
    }

    @Test
    public void callWithStrangeParameter() throws SQLException {
        try (CallableStatement stmt = sharedConnection.prepareCall("{call withStrangeParameter(?)}")) {
            double expected = 5.43;
            stmt.setDouble("a", expected);
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                double res = rs.getDouble(1);
                assertEquals(expected, res, 0);
                // now fail due to three decimals
                double tooMuch = 34.987;
                stmt.setDouble("a", tooMuch);
                try (ResultSet rs2 = stmt.executeQuery()) {
                    assertTrue(rs2.next());
                    assertThat(rs2.getDouble(1), is(not(tooMuch)));
                }
            }
        }
    }

    @Test
    public void meta() throws Exception {
        createProcedure("callabletest1", "()\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "callabletest1");
        if (rs.next()) {
            assertTrue("callabletest1".equals(rs.getString(3)));
        } else {
            fail();
        }
    }

    @Test
    public void testMetaCatalogNoAccessToProcedureBodies() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        Statement statement = sharedConnection.createStatement();
        try {
            statement.execute("DROP USER 'test_jdbc'@'%'");
        } catch (SQLException e) {
            //eat exception
        }
        statement.execute("CREATE USER 'test_jdbc'@'%' IDENTIFIED BY 'test_jdbc'");
        statement.execute("GRANT ALL PRIVILEGES ON testj.* TO 'test_jdbc'@'%' IDENTIFIED BY 'test_jdbc' WITH GRANT OPTION");
        Properties properties = new Properties();
        properties.put("user", "test_jdbc");
        properties.put("password", "test_jdbc");

        createProcedure("testMetaCatalog", "(x int, out y int)\nBEGIN\nSET y = 2;\n end\n");

        try (Connection connection = openConnection(connU, properties)) {
            CallableStatement callableStatement = connection.prepareCall("{call testMetaCatalog(?, ?)}");
            callableStatement.registerOutParameter(2, Types.INTEGER);
            try {
                callableStatement.setString("x", "1");
                fail("Set by named must not succeed");
            } catch (SQLException sqlException) {
                assertTrue(sqlException.getMessage().startsWith("Access to metaData informations not granted for current user"));
            }
            callableStatement.setString(1, "1");
            callableStatement.execute();
            try {
                callableStatement.getInt("y");
                fail("Get by named must not succeed");
            } catch (SQLException sqlException) {
                assertTrue(sqlException.getMessage().startsWith("Access to metaData informations not granted for current user"));
            }
            assertEquals(2, callableStatement.getInt(2));

            ResultSet resultSet = connection.getMetaData().getProcedures("yahoooo", null, "testMetaCatalog");
            assertFalse(resultSet.next());

            //test without catalog
            resultSet = connection.getMetaData().getProcedures(null, null, "testMetaCatalog");
            if (resultSet.next()) {
                assertTrue("testMetaCatalog".equals(resultSet.getString(3)));
                assertFalse(resultSet.next());
            } else {
                fail();
            }
        } catch (SQLInvalidAuthorizationSpecException authentication) {
            //MySQL 5.5 doesn't permit 'test_jdbc'@'localhost'
        }
        statement.execute("DROP USER 'test_jdbc'@'%'");
    }

    @Test
    public void testSameProcedureWithDifferentParameters() throws Exception {
        sharedConnection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS testj2");

        createProcedure("testj.testSameProcedureWithDifferentParameters",
                "(OUT p1 VARCHAR(10), IN p2 VARCHAR(10))\nBEGIN"
                        + "\nselect 1;"
                        + "\nEND");

        createProcedure("testj2.testSameProcedureWithDifferentParameters",
                "(OUT p1 VARCHAR(10))\nBEGIN"
                        + "\nselect 2;"
                        + "\nEND");

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters(?, ?) }")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.setString(2, "mike");
            callableStatement.execute();
        }
        sharedConnection.setCatalog("testj2");
        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters(?, ?) }")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.setString(2, "mike");
            try {
                callableStatement.execute();
                fail("Should've thrown an exception");
            } catch (SQLException sqlEx) {
                assertEquals("42000", sqlEx.getSQLState());
            }
        }

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters(?) }")) {
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.execute();
        }
        sharedConnection.setCatalog("testj");
        sharedConnection.createStatement().executeUpdate("DROP DATABASE testj2");
    }

    @Test
    public void testProcDecimalComa() throws Exception {
        createProcedure("testProcDecimalComa", "(decimalParam DECIMAL(18,0))\n"
                + "BEGIN\n"
                + "   SELECT 1;\n"
                + "END");
        try (CallableStatement callableStatement = sharedConnection.prepareCall("Call testProcDecimalComa(?)")) {
            callableStatement.setDouble(1, 18.0);
            callableStatement.execute();
        }
    }


    @Test
    public void testFunctionCall() throws Exception {
        createFunction("testFunctionCall", "(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
        CallableStatement callableStatement = sharedConnection.prepareCall("{? = CALL testFunctionCall(?,?,?)}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        callableStatement.setFloat(2, 2);
        callableStatement.setInt(3, 1);
        callableStatement.setInt(4, 1);

        assertEquals(4, callableStatement.getParameterMetaData().getParameterCount());
        assertEquals(Types.INTEGER, callableStatement.getParameterMetaData().getParameterType(1));
        DatabaseMetaData dbmd = sharedConnection.getMetaData();

        ResultSet rs = dbmd.getFunctionColumns(sharedConnection.getCatalog(), null, "testFunctionCall", "%");
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(17, rsmd.getColumnCount());
        assertEquals("FUNCTION_CAT", rsmd.getColumnName(1));
        assertEquals("FUNCTION_SCHEM", rsmd.getColumnName(2));
        assertEquals("FUNCTION_NAME", rsmd.getColumnName(3));
        assertEquals("COLUMN_NAME", rsmd.getColumnName(4));
        assertEquals("COLUMN_TYPE", rsmd.getColumnName(5));
        assertEquals("DATA_TYPE", rsmd.getColumnName(6));
        assertEquals("TYPE_NAME", rsmd.getColumnName(7));
        assertEquals("PRECISION", rsmd.getColumnName(8));
        assertEquals("LENGTH", rsmd.getColumnName(9));
        assertEquals("SCALE", rsmd.getColumnName(10));
        assertEquals("RADIX", rsmd.getColumnName(11));
        assertEquals("NULLABLE", rsmd.getColumnName(12));
        assertEquals("REMARKS", rsmd.getColumnName(13));
        assertEquals("CHAR_OCTET_LENGTH", rsmd.getColumnName(14));
        assertEquals("ORDINAL_POSITION", rsmd.getColumnName(15));
        assertEquals("IS_NULLABLE", rsmd.getColumnName(16));
        assertEquals("SPECIFIC_NAME", rsmd.getColumnName(17));

        rs.close();

        assertFalse(callableStatement.execute());
        assertEquals(2f, callableStatement.getInt(1), .001);
        assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(-1, callableStatement.executeUpdate());
        assertEquals(2f, callableStatement.getInt(1), .001);
        assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        callableStatement.setFloat("a", 4);
        callableStatement.setInt("b", 1);
        callableStatement.setInt("c", 1);

        assertFalse(callableStatement.execute());
        assertEquals(4f, callableStatement.getInt(1), .001);
        assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(-1, callableStatement.executeUpdate());
        assertEquals(4f, callableStatement.getInt(1), .001);
        assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());


        rs = dbmd.getProcedures(sharedConnection.getCatalog(), null, "testFunctionCall");
        assertTrue(rs.next());
        assertEquals("testFunctionCall", rs.getString("PROCEDURE_NAME"));
        assertEquals(DatabaseMetaData.procedureReturnsResult, rs.getShort("PROCEDURE_TYPE"));
        callableStatement.setNull(2, Types.FLOAT);
        callableStatement.setInt(3, 1);
        callableStatement.setInt(4, 1);

        assertFalse(callableStatement.execute());
        assertEquals(0f, callableStatement.getInt(1), .001);
        assertEquals(true, callableStatement.wasNull());
        assertEquals(null, callableStatement.getObject(1));
        assertEquals(true, callableStatement.wasNull());

        assertEquals(-1, callableStatement.executeUpdate());
        assertEquals(0f, callableStatement.getInt(1), .001);
        assertEquals(true, callableStatement.wasNull());
        assertEquals(null, callableStatement.getObject(1));
        assertEquals(true, callableStatement.wasNull());

        callableStatement = sharedConnection.prepareCall("{? = CALL testFunctionCall(4,5,?)}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        callableStatement.setInt(2, 1);

        assertFalse(callableStatement.execute());
        assertEquals(4f, callableStatement.getInt(1), .001);
        assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(-1, callableStatement.executeUpdate());
        assertEquals(4f, callableStatement.getInt(1), .001);
        assertEquals("java.lang.Integer", callableStatement.getObject(1).getClass().getName());

        assertEquals(4, callableStatement.getParameterMetaData().getParameterCount());
        assertEquals(Types.INTEGER, callableStatement.getParameterMetaData().getParameterType(1));
        assertEquals(Types.FLOAT, callableStatement.getParameterMetaData().getParameterType(2));
        assertEquals(Types.BIGINT, callableStatement.getParameterMetaData().getParameterType(3));
        assertEquals(Types.INTEGER, callableStatement.getParameterMetaData().getParameterType(4));
    }

    @Test
    public void testCallOtherDb() throws Exception {
        sharedConnection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS testj2");
        createProcedure("testj2.otherDbProcedure", "()\nBEGIN\nSELECT 1;\nEND ");

        try (Connection noDbConn = setConnection()) {
            noDbConn.prepareCall("{call `testj2`.otherDbProcedure()}").execute();
        }
        sharedConnection.createStatement().executeUpdate("DROP DATABASE testj2");
    }

    @Test
    public void testMultiResultset() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("testInOutParam", "(IN p1 VARCHAR(255), INOUT p2 INT)\n"
                + "begin\n"
                + " DECLARE z INT;\n"
                + " SET z = p2 + 1;\n"
                + " SET p2 = z;\n"
                + " SELECT p1;\n"
                + " SELECT CONCAT('todo ', p1);\n"
                + "end");
        try (CallableStatement callableStatement = sharedConnection.prepareCall("{call testInOutParam(?, ?)}")) {
            callableStatement.registerOutParameter(2, Types.INTEGER);
            callableStatement.setString(1, "test");
            callableStatement.setInt(2, 1);
            ResultSet resultSet = callableStatement.executeQuery();
            assertEquals(2, callableStatement.getInt(2));
            if (resultSet.next()) {
                assertEquals("test", resultSet.getString(1));
            } else {
                fail("must have resultset");
            }
            assertTrue(callableStatement.getMoreResults());

            resultSet = callableStatement.getResultSet();
            if (resultSet.next()) {
                assertEquals("todo test", resultSet.getString(1));
            } else {
                fail("must have resultset");
            }
        }
    }

    @Test
    public void callFunctionWithNoParameters() throws SQLException {
        createFunction("callFunctionWithNoParameters", "()\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN 'mike';");

        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call callFunctionWithNoParameters()}");
        callableStatement.registerOutParameter(1, Types.VARCHAR);
        callableStatement.execute();
        assertEquals("mike", callableStatement.getString(1));
    }

    @Test
    public void testFunctionWith2parameters() throws SQLException {
        createFunction("testFunctionWith2parameters", "(s CHAR(20), s2 CHAR(20))\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN CONCAT(s,' and ', s2)");

        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call testFunctionWith2parameters(?, ?)}");
        callableStatement.registerOutParameter(1, Types.VARCHAR);
        callableStatement.setString(2, "mike");
        callableStatement.setString(3, "bart");
        callableStatement.execute();
        assertEquals("mike and bart", callableStatement.getString(1));
    }

    @Test
    public void testFunctionWithFixedParameters() throws SQLException {
        createFunction("testFunctionWith2parameters", "(s CHAR(20), s2 CHAR(20))\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN CONCAT(s,' and ', s2)");

        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call testFunctionWith2parameters('mike', ?)}");
        callableStatement.registerOutParameter(1, Types.VARCHAR);
        callableStatement.setString(2, "bart");
        callableStatement.execute();
        assertEquals("mike and bart", callableStatement.getString(1));
    }

    @Test
    public void testResultsetWithInoutParameter() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createTable("testResultsetWithInoutParameterTb", "test VARCHAR(10)");
        createProcedure("testResultsetWithInoutParameter", "(INOUT testValue VARCHAR(10))\n"
                + "BEGIN\n"
                + " insert into testResultsetWithInoutParameterTb(test) values (testValue);\n"
                + " SELECT testValue;\n"
                + " SET testValue = UPPER(testValue);\n"
                + "END");
        CallableStatement cstmt = sharedConnection.prepareCall("{call testResultsetWithInoutParameter(?)}");
        cstmt.registerOutParameter(1, Types.VARCHAR);
        cstmt.setString(1, "mike");
        //assertEquals(1, cstmt.executeUpdate());
        cstmt.executeUpdate();
        assertEquals("MIKE", cstmt.getString(1));
        //assertTrue(cstmt.getMoreResults());
        ResultSet resultSet = cstmt.getResultSet();
        if (resultSet.next()) {
            assertEquals("mike", resultSet.getString(1));
        } else {
            fail("must have a resultset corresponding to the SELECT testValue");
        }
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT * FROM testResultsetWithInoutParameterTb");
        if (rs.next()) {
            assertEquals("mike", rs.getString(1));
        } else {
            fail();
        }
    }

    @Test
    public void testSettingFixedParameter() throws SQLException {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        try (Connection connection = setConnection()) {
            createProcedure("simpleproc", "(IN inParam CHAR(20), INOUT inOutParam CHAR(20), OUT outParam CHAR(50))"
                    + "     BEGIN\n"
                    + "         SET inOutParam = UPPER(inOutParam);\n"
                    + "         SET outParam = CONCAT('Hello, ', inOutParam, ' and ', inParam);"
                    + "         SELECT 'a' FROM DUAL;\n"
                    + "     END;");

            CallableStatement callableStatement = connection.prepareCall("{call simpleproc('mike', ?, ?)}");
            callableStatement.registerOutParameter(1, Types.VARCHAR);
            callableStatement.registerOutParameter(2, Types.VARCHAR);
            callableStatement.setString(1, "toto");
            callableStatement.execute();
            String result = callableStatement.getString(1);
            String result2 = callableStatement.getString(2);
            if (!"TOTO".equals(result) && !"Hello, TOTO and mike".equals(result2)) {
                fail();
            }
            callableStatement.close();
        }
    }

    @Test
    public void testNoParenthesisCall() throws Exception {
        createProcedure("testProcedureParenthesis", "() BEGIN SELECT 1; END");
        createFunction("testFunctionParenthesis", "() RETURNS INT DETERMINISTIC RETURN 1;");
        sharedConnection.prepareCall("{CALL testProcedureParenthesis}").execute();
        sharedConnection.prepareCall("{? = CALL testFunctionParenthesis}").execute();
    }

    @Test
    public void testProcLinefeed() throws Exception {
        createProcedure("testProcLinefeed", "(\r\n)\r\n BEGIN SELECT 1; END");
        CallableStatement callStmt = sharedConnection.prepareCall("{CALL testProcLinefeed()}");
        callStmt.execute();

        sharedConnection.createStatement().executeUpdate("DROP PROCEDURE IF EXISTS testProcLinefeed");
        sharedConnection.createStatement().executeUpdate("CREATE PROCEDURE testProcLinefeed(\r\na INT)\r\n BEGIN SELECT 1; END");
        callStmt = sharedConnection.prepareCall("{CALL testProcLinefeed(?)}");
        callStmt.setInt(1, 1);
        callStmt.execute();
    }


    @Test
    public void testHugeNumberOfParameters() throws Exception {
        StringBuilder procDef = new StringBuilder("(");
        StringBuilder param = new StringBuilder();
        for (int i = 0; i < 274; i++) {
            if (i != 0) {
                procDef.append(",");
                param.append(",");
            }

            procDef.append(" OUT param_").append(i).append(" VARCHAR(32)");
            param.append("?");
        }

        procDef.append(")\nBEGIN\nSELECT 1;\nEND");
        createProcedure("testHugeNumberOfParameters", procDef.toString());

        try (CallableStatement callableStatement = sharedConnection.prepareCall(
                "{call testHugeNumberOfParameters(" + param.toString() + ")}")) {
            callableStatement.registerOutParameter(274, Types.VARCHAR);
            callableStatement.execute();
        }
    }

    @Test
    public void testStreamInOutWithName() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("testStreamInOutWithName", "(INOUT mblob MEDIUMBLOB) BEGIN SELECT 1 FROM DUAL WHERE 1=0;\nEND");
        try (CallableStatement cstmt = sharedConnection.prepareCall("{call testStreamInOutWithName(?)}")) {
            byte[] buffer = new byte[65];
            for (int i = 0; i < 65; i++) {
                buffer[i] = 1;
            }
            int il = buffer.length;
            int[] typesToTest = new int[]{Types.BIT, Types.BINARY, Types.BLOB, Types.JAVA_OBJECT, Types.LONGVARBINARY, Types.VARBINARY};

            for (int typeToTest : typesToTest) {
                cstmt.setBinaryStream("mblob", new ByteArrayInputStream(buffer), buffer.length);
                cstmt.registerOutParameter("mblob", typeToTest);
                cstmt.executeUpdate();

                InputStream is = cstmt.getBlob("mblob").getBinaryStream();
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

                int bytesRead;
                byte[] readBuf = new byte[256];

                while ((bytesRead = is.read(readBuf)) != -1) {
                    byteArrayOutputStream.write(readBuf, 0, bytesRead);
                }

                byte[] fromSelectBuf = byteArrayOutputStream.toByteArray();
                int ol = fromSelectBuf.length;
                assertEquals(il, ol);
            }

            cstmt.close();
        }
    }

    @Test
    public void testDefinerCallableStatement() throws Exception {
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate("DROP PROCEDURE IF EXISTS testDefinerCallableStatement");
        stmt.executeUpdate("CREATE DEFINER=CURRENT_USER PROCEDURE testDefinerCallableStatement(I INT) COMMENT 'abcdefg'\nBEGIN\nSELECT I * 10;\nEND");
        sharedConnection.prepareCall("{call testDefinerCallableStatement(?)}").close();
        stmt.executeUpdate("DROP PROCEDURE IF EXISTS testDefinerCallableStatement");
    }

    @Test
    public void testProcedureComment() throws Exception {
        createProcedure("testProcedureComment", "(a INT, b VARCHAR(32)) BEGIN SELECT CONCAT(CONVERT(a, CHAR(50)), b); END");

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call /*comment ? */ testj.testProcedureComment(?, "
                + "/*comment ? */?) #comment ? }")) {
            assertTrue(callableStatement.toString().contains("/*"));
            callableStatement.setInt(1, 1);
            callableStatement.setString(2, " a");
            ResultSet rs = callableStatement.executeQuery();
            if (rs.next()) {
                assertEquals("1 a", rs.getString(1));
            } else {
                fail("must have a result !");
            }
        }
    }

    @Test
    public void testCommentParser() throws Exception {
        createProcedure("testCommentParser", "(_ACTION varchar(20),"
                + "`/*dumb-identifier-1*/` int,"
                + "\n`#dumb-identifier-2` int,"
                + "\n`--dumb-identifier-3` int,"
                + "\n_CLIENT_ID int, -- ABC"
                + "\n_LOGIN_ID  int, # DEF"
                + "\n_WHERE varchar(2000),"
                + "\n_SORT varchar(2000),"
                + "\n out _SQL varchar(/* inline right here - oh my gosh! */ 8000),"
                + "\n _SONG_ID int,"
                + "\n  _NOTES varchar(2000),"
                + "\n out _RESULT varchar(10)"
                + "\n /*"
                + "\n ,    -- Generic result parameter"
                + "\n out _PERIOD_ID int,         -- Returns the period_id. "
                + "Useful when using @PREDEFLINK to return which is the last period"
                + "\n   _SONGS_LIST varchar(8000),"
                + "\n  _COMPOSERID int,"
                + "\n  _PUBLISHERID int,"
                + "\n   _PREDEFLINK int        -- If the user is accessing through a "
                + "predefined link: 0=none  1=last period"
                + "\n */) BEGIN SELECT 1; END");

        createProcedure("testCommentParser_1", "(`/*id*/` /* before type 1 */ varchar(20),"
                + "/* after type 1 */ OUT result2 DECIMAL(/*size1*/10,/*size2*/2) /* p2 */)BEGIN SELECT action, result; END");

        sharedConnection.prepareCall("{call testCommentParser(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}").close();
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(sharedConnection.getCatalog(), null, "testCommentParser", "%");
        validateResult(rs,
                new String[]{
                        "_ACTION",
                        "/*dumb-identifier-1*/",
                        "#dumb-identifier-2",
                        "--dumb-identifier-3",
                        "_CLIENT_ID",
                        "_LOGIN_ID",
                        "_WHERE",
                        "_SORT",
                        "_SQL",
                        "_SONG_ID",
                        "_NOTES",
                        "_RESULT"},
                new int[]{Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR},
                new int[]{20, 10, 10, 10, 10, 10, 2000, 2000, 8000, 10, 2000, 10},
                new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new int[]{DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnOut,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnIn,
                        DatabaseMetaData.procedureColumnOut});

        sharedConnection.prepareCall("{call testCommentParser_1(?, ?)}").close();
        rs = sharedConnection.getMetaData().getProcedureColumns(sharedConnection.getCatalog(), null, "testCommentParser_1", "%");
        validateResult(rs,
                new String[]{"/*id*/", "result2"},
                new int[]{Types.VARCHAR, Types.DECIMAL},
                new int[]{20, 10},
                new int[]{0, 2},
                new int[]{DatabaseMetaData.procedureColumnIn, DatabaseMetaData.procedureColumnOut});
    }

    private void validateResult(ResultSet rs, String[] parameterNames, int[] parameterTypes, int[] precision,
                                int[] scale, int[] direction) throws SQLException {
        int index = 0;
        while (rs.next()) {
            assertEquals(parameterNames[index], rs.getString("COLUMN_NAME"));
            assertEquals(parameterTypes[index], rs.getInt("DATA_TYPE"));

            switch (index) {
                
                case 0:
                case 6:
                case 7:
                case 8:
                case 10:
                case 11:
                    assertEquals(precision[index], rs.getInt("LENGTH"));
                    break;

                default:
                    assertEquals(precision[index], rs.getInt("PRECISION"));
                    break;
            }
            assertEquals(scale[index], rs.getInt("SCALE"));
            assertEquals(direction[index], rs.getInt("COLUMN_TYPE"));

            index++;
        }
        rs.close();
    }

    @Test
    public void testCallableNullSetters() throws Throwable {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createTable("testCallableNullSettersTable", "value_1 BIGINT PRIMARY KEY,value_2 VARCHAR(20)");
        createFunction("testCallableNullSetters", "(value_1_v BIGINT, value_2_v VARCHAR(20)) RETURNS BIGINT "
                + "DETERMINISTIC MODIFIES SQL DATA BEGIN "
                + "INSERT INTO testCallableNullSettersTable VALUES (value_1_v,value_2_v); "
                + "RETURN value_1_v; "
                + "END;");
        createProcedure("testCallableNullSettersProc", "(OUT value_1_v BIGINT, IN value_2_v BIGINT, IN value_3_v VARCHAR(20)) "
                + "BEGIN "
                + "INSERT INTO testCallableNullSettersTable VALUES (value_2_v,value_3_v); "
                + "SET value_1_v = value_2_v; "
                + "END;");
        // Prepare the function call
        try (CallableStatement callable = sharedConnection.prepareCall("{? = call testCallableNullSetters(?,?)}")) {
            testSetter(callable);
        }
        sharedConnection.createStatement().execute("TRUNCATE testCallableNullSettersTable");
        // Prepare the procedure call
        try (CallableStatement callable = sharedConnection.prepareCall("{call testCallableNullSettersProc(?,?,?)}")) {
            testSetter(callable);
        }
    }

    private void testSetter(CallableStatement callable) throws Throwable {
        callable.registerOutParameter(1, Types.BIGINT);

        // Add row with non-null value
        callable.setLong(2, 1);
        callable.setString(3, "Non-null value");
        callable.executeUpdate();
        assertEquals(1, callable.getLong(1));

        // Add row with null value
        callable.setLong(2, 2);
        callable.setNull(3, Types.VARCHAR);
        callable.executeUpdate();
        assertEquals(2, callable.getLong(1));

        Method[] setters = CallableStatement.class.getMethods();

        for (Method setter : setters) {
            if (setter.getName().startsWith("set")) {
                Class<?>[] args = setter.getParameterTypes();

                if (args.length == 2 && args[0].equals(Integer.TYPE)) {
                    if (!args[1].isPrimitive()) {
                        try {
                            setter.invoke(callable, 2, null);
                        } catch (InvocationTargetException ive) {
                            if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                throw ive;
                            }
                        }
                    } else {
                        if (args[1].getName().equals("boolean")) {
                            try {
                                setter.invoke(callable, 2, Boolean.FALSE);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }
                        }

                        if (args[1].getName().equals("byte")) {

                            try {
                                setter.invoke(callable, 2, (byte) 0);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("double")) {

                            try {
                                setter.invoke(callable, 2, 0D);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("float")) {

                            try {
                                setter.invoke(callable, 2, 0f);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("int")) {

                            try {
                                setter.invoke(callable, 2, 0);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("long")) {
                            try {
                                setter.invoke(callable, 2, 0L);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }
                        }

                        if (args[1].getName().equals("short")) {
                            try {
                                setter.invoke(callable, 2, (short) 0);
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCallableThrowException() throws Exception {
        createTable("testCallableThrowException1", "value_1 BIGINT PRIMARY KEY", "ENGINE=InnoDB");
        createTable("testCallableThrowException2", "value_2 BIGINT PRIMARY KEY", "ENGINE=InnoDB");

        sharedConnection.createStatement().executeUpdate("INSERT INTO testCallableThrowException1 VALUES (1)");
        createFunction("test_function", "() RETURNS BIGINT DETERMINISTIC MODIFIES SQL DATA BEGIN DECLARE max_value BIGINT; "
                + "SELECT MAX(value_1) INTO max_value FROM testCallableThrowException2; RETURN max_value; END;");

        try (CallableStatement callable = sharedConnection.prepareCall("{? = call test_function()}")) {

            callable.registerOutParameter(1, Types.BIGINT);

            try {
                callable.executeUpdate();
                fail("impossible; we should never get here.");
            } catch (SQLException sqlEx) {
                assertEquals("42S22", sqlEx.getSQLState());
            }
        }

        sharedConnection.createStatement().execute("DROP TABLE IF EXISTS testCallableThrowException4");
        createTable("testCallableThrowException3", "value_1 BIGINT PRIMARY KEY", "ENGINE=InnoDB");
        sharedConnection.createStatement().executeUpdate("INSERT INTO testCallableThrowException3 VALUES (1)");
        createTable("testCallableThrowException4", "value_2 BIGINT PRIMARY KEY, "
                + " FOREIGN KEY (value_2) REFERENCES testCallableThrowException3 (value_1) ON DELETE CASCADE", "ENGINE=InnoDB");
        createFunction("test_function", "(value BIGINT) RETURNS BIGINT DETERMINISTIC MODIFIES SQL DATA BEGIN "
                + "INSERT INTO testCallableThrowException4 VALUES (value); RETURN value; END;");

        try (CallableStatement callable = sharedConnection.prepareCall("{? = call test_function(?)}")) {
            callable.registerOutParameter(1, Types.BIGINT);
            callable.setLong(2, 1);
            callable.executeUpdate();
            callable.setLong(2, 2);
            try {
                callable.executeUpdate();
                fail("impossible; we should never get here.");
            } catch (SQLException sqlEx) {
                assertEquals("23000", sqlEx.getSQLState());
            }
        }
    }

    @Test
    public void testCallableStatementFormat() throws Exception {
        try {
            sharedConnection.prepareCall("CREATE TABLE testCallableStatementFormat(id INT)");
        } catch (Exception exception) {
            assertTrue(exception.getMessage().startsWith("invalid callable syntax"));
        }
    }

    @Test
    public void testFunctionWithFixedParameter() throws Exception {
        createFunction("testFunctionWithFixedParameter", "(a varchar(40), b bigint(20), c varchar(80)) RETURNS bigint(20) LANGUAGE SQL DETERMINISTIC "
                + "MODIFIES SQL DATA COMMENT 'bbb' BEGIN RETURN 1; END; ");

        try (CallableStatement callable = sharedConnection.prepareCall("{? = call testFunctionWithFixedParameter(?,101,?)}")) {
            callable.registerOutParameter(1, Types.BIGINT);
            callable.setString(2, "FOO");
            callable.setString(3, "BAR");
            callable.executeUpdate();
        }
    }


    @Test
    public void testParameterNumber() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createTable("TMIX91P", "F01SMALLINT         SMALLINT NOT NULL, F02INTEGER          INTEGER,F03REAL             REAL,"
                + "F04FLOAT            FLOAT,F05NUMERIC31X4      NUMERIC(31,4), F06NUMERIC16X16     NUMERIC(16,16), F07CHAR_10          CHAR(10),"
                + " F08VARCHAR_10       VARCHAR(10), F09CHAR_20          CHAR(20), F10VARCHAR_20       VARCHAR(20), F11DATE         DATE,"
                + " F12DATETIME         DATETIME, PRIMARY KEY (F01SMALLINT)");
        Statement stmt = sharedConnection.createStatement();
        stmt.executeUpdate("INSERT INTO TMIX91P VALUES (1,1,1234567.12,1234567.12,111111111111111111111111111.1111,.111111111111111,'1234567890',"
                + "'1234567890','CHAR20CHAR20','VARCHAR20ABCD','2001-01-01','2001-01-01 01:01:01.111')");

        stmt.executeUpdate("INSERT INTO TMIX91P VALUES (7,1,1234567.12,1234567.12,22222222222.0001,.99999999999,'1234567896','1234567896','CHAR20',"
                + "'VARCHAR20ABCD','2001-01-01','2001-01-01 01:01:01.111')");

        stmt.executeUpdate("INSERT INTO TMIX91P VALUES (12,12,1234567.12,1234567.12,111222333.4444,.1234567890,'2234567891','2234567891','CHAR20',"
                + "'VARCHAR20VARCHAR20','2001-01-01','2001-01-01 01:01:01.111')");

        createProcedure("MSQSPR100", "\n( p1_in  INTEGER , p2_in  CHAR(20), OUT p3_out INTEGER, OUT p4_out CHAR(11))\nBEGIN "
                + "\n SELECT F01SMALLINT,F02INTEGER, F11DATE,F12DATETIME,F03REAL \n FROM TMIX91P WHERE F02INTEGER = p1_in; "
                + "\n SELECT F02INTEGER,F07CHAR_10,F08VARCHAR_10,F09CHAR_20 \n FROM TMIX91P WHERE  F09CHAR_20 = p2_in ORDER BY F02INTEGER ; "
                + "\n SET p3_out  = 144; \n SET p4_out  = 'CHARACTER11'; \n SELECT p3_out, p4_out; END");

        String sql = "{call MSQSPR100(1,'CHAR20',?,?)}";

        CallableStatement cs = sharedConnection.prepareCall(sql);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.CHAR);

        cs.execute();
        cs.close();

        createProcedure("testParameterNumber_1",
                "(OUT nfact VARCHAR(100), IN ccuenta VARCHAR(100),\nOUT ffact VARCHAR(100),\nOUT fdoc VARCHAR(100))\nBEGIN"
                        + "\nSET nfact = 'ncfact string';\nSET ffact = 'ffact string';\nSET fdoc = 'fdoc string';\nEND");

        createProcedure("testParameterNumber_2",
                "(IN ccuent1 VARCHAR(100), IN ccuent2 VARCHAR(100),\nOUT nfact VARCHAR(100),\nOUT ffact VARCHAR(100),"
                        + "\nOUT fdoc VARCHAR(100))\nBEGIN\nSET nfact = 'ncfact string';\nSET ffact = 'ffact string';\n"
                        + "SET fdoc = 'fdoc string';\nEND");

        Properties props = new Properties();
        props.put("jdbcCompliantTruncation", "true");
        props.put("useInformationSchema", "true");
        try (Connection conn1 = setConnection(props)) {
            CallableStatement callSt = conn1.prepareCall("{ call testParameterNumber_1(?, ?, ?, ?) }");
            callSt.setString(2, "xxx");
            callSt.registerOutParameter(1, Types.VARCHAR);
            callSt.registerOutParameter(3, Types.VARCHAR);
            callSt.registerOutParameter(4, Types.VARCHAR);
            callSt.execute();

            assertEquals("ncfact string", callSt.getString(1));
            assertEquals("ffact string", callSt.getString(3));
            assertEquals("fdoc string", callSt.getString(4));

            CallableStatement callSt2 = conn1.prepareCall("{ call testParameterNumber_2(?, ?, ?, ?, ?) }");
            callSt2.setString(1, "xxx");
            callSt2.setString(2, "yyy");
            callSt2.registerOutParameter(3, Types.VARCHAR);
            callSt2.registerOutParameter(4, Types.VARCHAR);
            callSt2.registerOutParameter(5, Types.VARCHAR);
            callSt2.execute();

            assertEquals("ncfact string", callSt2.getString(3));
            assertEquals("ffact string", callSt2.getString(4));
            assertEquals("fdoc string", callSt2.getString(5));

            CallableStatement callSt3 = conn1.prepareCall("{ call testParameterNumber_2(?, 'yyy', ?, ?, ?) }");
            callSt3.setString(1, "xxx");
            // callSt3.setString(2, "yyy");
            callSt3.registerOutParameter(2, Types.VARCHAR);
            callSt3.registerOutParameter(3, Types.VARCHAR);
            callSt3.registerOutParameter(4, Types.VARCHAR);
            callSt3.execute();

            assertEquals("ncfact string", callSt3.getString(2));
            assertEquals("ffact string", callSt3.getString(3));
            assertEquals("fdoc string", callSt3.getString(4));
        }
    }

    @Test
    public void testProcMultiDb() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        String originalCatalog = sharedConnection.getCatalog();

        sharedConnection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS testProcMultiDb");

        createProcedure("testProcMultiDb.testProcMultiDbProc", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        CallableStatement callableStatement = null;
        try {
            callableStatement = sharedConnection.prepareCall("{call `testProcMultiDb`.`testProcMultiDbProc`(?, ?)}");
            callableStatement.setInt(1, 5);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            assertEquals(6, callableStatement.getInt(2));
            callableStatement.clearParameters();
            callableStatement.close();

            sharedConnection.setCatalog("testProcMultiDb");
            callableStatement = sharedConnection.prepareCall("{call testProcMultiDb.testProcMultiDbProc(?, ?)}");
            callableStatement.setInt(1, 5);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            assertEquals(6, callableStatement.getInt(2));
            callableStatement.clearParameters();
            callableStatement.close();

            sharedConnection.setCatalog("mysql");
            callableStatement = sharedConnection.prepareCall("{call `testProcMultiDb`.`testProcMultiDbProc`(?, ?)}");
            callableStatement.setInt(1, 5);
            callableStatement.registerOutParameter(2, Types.INTEGER);

            callableStatement.execute();
            assertEquals(6, callableStatement.getInt(2));
        } finally {
            assert callableStatement != null;
            callableStatement.clearParameters();
            callableStatement.close();
            sharedConnection.setCatalog(originalCatalog);
            sharedConnection.createStatement().executeUpdate("DROP DATABASE testProcMultiDb");
        }
    }


    @Test
    public void callProcSendNullInOut() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("testProcSendNullInOut_1", "(INOUT x INTEGER)\nBEGIN\nSET x = x + 1;\nEND");
        createProcedure("testProcSendNullInOut_2", "(x INTEGER, OUT y INTEGER)\nBEGIN\nSET y = x + 1;\nEND");
        createProcedure("testProcSendNullInOut_3", "(INOUT x INTEGER)\nBEGIN\nSET x = 10;\nEND");

        CallableStatement call = sharedConnection.prepareCall("{ call testProcSendNullInOut_1(?) }");
        call.registerOutParameter(1, Types.INTEGER);
        call.setInt(1, 1);
        call.execute();
        assertEquals(2, call.getInt(1));

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_2(?, ?) }");
        call.registerOutParameter(2, Types.INTEGER);
        call.setInt(1, 1);
        call.execute();
        assertEquals(2, call.getInt(2));

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_2(?, ?) }");
        call.registerOutParameter(2, Types.INTEGER);
        call.setNull(1, Types.INTEGER);
        call.execute();
        assertEquals(0, call.getInt(2));
        assertTrue(call.wasNull());

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_1(?) }");
        call.registerOutParameter(1, Types.INTEGER);
        call.setNull(1, Types.INTEGER);
        call.execute();
        assertEquals(0, call.getInt(1));
        assertTrue(call.wasNull());

        call = sharedConnection.prepareCall("{ call testProcSendNullInOut_3(?) }");
        call.registerOutParameter(1, Types.INTEGER);
        call.setNull(1, Types.INTEGER);
        call.execute();
        assertEquals(10, call.getInt(1));
    }

    /**
     * CONJ-263: Error in stored procedure or SQL statement with allowMultiQueries does not raise Exception
     * when there is a result returned prior to erroneous statement.
     *
     * @throws SQLException exception
     */
    @Test
    public void testCallExecuteErrorBatch() throws SQLException {
        CallableStatement callableStatement = sharedConnection.prepareCall("{call TEST_SP1()}");
        try {
            callableStatement.execute();
            fail("Must have thrown error");
        } catch (SQLException sqle) {
            //must have thrown error.
            assertTrue(sqle.getMessage().contains("Test error from SP"));
        }
    }

    /**
     * CONJ-298 : Callable function exception when no parameter and space before parenthesis.
     *
     * @throws SQLException exception
     */
    @Test
    public void testFunctionWithSpace() throws SQLException {
        createFunction("hello", "()\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN CONCAT('Hello, !');");
        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call `hello` ()}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        assertFalse(callableStatement.execute());
        assertEquals("Hello, !", callableStatement.getString(1));
    }

    /**
     * CONJ-425 : take care of registerOutParameter type.
     *
     * @throws Exception if connection error occur
     */
    @Test
    public void testOutputObjectType() throws Exception {
        //cancel for version 10.2 beta before fix https://jira.mariadb.org/browse/MDEV-11761
        cancelForVersion(10, 2, 2);
        cancelForVersion(10, 2, 3);
        cancelForVersion(10, 2, 4);

        createProcedure("issue425", "(IN inValue TEXT, OUT testValue TEXT)\n"
                + "BEGIN\n"
                + " set testValue = CONCAT('o', inValue);\n"
                + "END");

        //registering with VARCHAR Type
        CallableStatement cstmt = sharedConnection.prepareCall("{call issue425(?, ?)}");
        cstmt.registerOutParameter(2, Types.VARCHAR);
        cstmt.setString(1, "x");
        cstmt.execute();

        assertEquals("ox", cstmt.getString(2));
        assertEquals("ox", cstmt.getObject(2, String.class)); //works
        assertEquals("ox", cstmt.getObject(2));
        assertEquals("ox", cstmt.getObject("testValue"));

        //registering with Binary Type
        CallableStatement cstmt2 = sharedConnection.prepareCall("{call issue425(?, ?)}");
        cstmt2.registerOutParameter(2, Types.BINARY);
        cstmt2.setString(1, "x");
        cstmt2.execute();

        assertEquals("ox", cstmt2.getString(2));
        assertEquals("ox", cstmt2.getObject(2, String.class)); //works
        assertTrue(cstmt2.getObject(2) instanceof byte[]);
        assertArrayEquals("ox".getBytes(), ((byte[]) cstmt2.getObject(2)));
        assertArrayEquals("ox".getBytes(), ((byte[]) cstmt2.getObject("testValue")));

    }

    @Test
    public void testOutputObjectTypeFunction() throws Exception {
        createFunction("issue425f", "(a TEXT, b TEXT) RETURNS TEXT NO SQL\nBEGIN\nRETURN CONCAT(a, b);\nEND");

        //registering with VARCHAR Type
        CallableStatement cstmt = sharedConnection.prepareCall("{? = call issue425f(?, ?)}");
        cstmt.registerOutParameter(1, Types.VARCHAR);
        cstmt.setString(2, "o");
        cstmt.setString(3, "x");
        cstmt.execute();

        assertEquals("ox", cstmt.getString(1));
        assertEquals("ox", cstmt.getObject(1, String.class)); //works
        assertEquals("ox", cstmt.getObject(1));

        //registering with Binary Type
        CallableStatement cstmt2 = sharedConnection.prepareCall("{? = call issue425f(?, ?)}");
        cstmt2.registerOutParameter(1, Types.BINARY);
        cstmt2.setString(2, "o");
        cstmt2.setString(3, "x");
        cstmt2.execute();

        assertEquals("ox", cstmt2.getString(1));
        assertEquals("ox", cstmt2.getObject(1, String.class)); //works
        assertTrue(cstmt2.getObject(1) instanceof byte[]);
        assertArrayEquals("ox".getBytes(), ((byte[]) cstmt2.getObject(1)));

    }

    @Test
    public void procedureCaching() throws SQLException {
        createProcedure("cacheCall", "(IN inValue int)\n"
                + "BEGIN\n"
                + " /*do nothing*/ \n"
                + "END");

        CallableStatement st = sharedConnection.prepareCall("{call testj.cacheCall(?)}");
        st.setInt(1, 2);
        st.execute();

        try (CallableStatement st2 = sharedConnection.prepareCall("{call testj.cacheCall(?)}")) {
            st2.setInt(1, 2);
            st2.execute();
            st.close();

            try (CallableStatement st3 = sharedConnection.prepareCall("{call testj.cacheCall(?)}")) {
                st3.setInt(1, 2);
                st3.execute();
                st3.execute();
            }
        }

        try (CallableStatement st3 = sharedConnection.prepareCall("{?=call pow(?,?)}")) {
            st3.setInt(2, 2);
            st3.setInt(3, 2);
            st3.execute();
        }
    }

    @Test
    public void functionCaching() throws SQLException {
        createFunction("hello2", "()\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN CONCAT('Hello, !');");
        CallableStatement st = sharedConnection.prepareCall("{? = call hello2()}");
        st.registerOutParameter(1, Types.INTEGER);
        assertFalse(st.execute());

        try (CallableStatement st2 = sharedConnection.prepareCall("{? = call hello2()}")) {
            st2.registerOutParameter(1, Types.INTEGER);
            assertFalse(st2.execute());

            st.close();

            try (CallableStatement st3 = sharedConnection.prepareCall("{? = call hello2()}")) {
                st3.registerOutParameter(1, Types.INTEGER);
                assertFalse(st3.execute());
            }
        }

        try (CallableStatement st3 = sharedConnection.prepareCall("{? = call hello2()}")) {
            st3.registerOutParameter(1, Types.INTEGER);
            assertFalse(st3.execute());
        }
    }
}
