package org.mariadb.jdbc;


import org.junit.Assert;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class CallableStatementTest extends BaseTest {
    /**
     * Initialisation.
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createProcedure("withResultSet", "(a int) begin select a; end");
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
    }

    @Before
    public void checkSp() throws SQLException {
        requireMinimumVersion(5, 0);
    }

    @Test
    public void callSimple() throws SQLException {
        CallableStatement st = sharedConnection.prepareCall("{?=call pow(?,?)}");
        st.setInt(2, 2);
        st.setInt(3, 2);
        st.execute();
        int result = st.getInt(1);
        assertEquals(result, 4);

    }


    @Test
    public void withResultSet() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call withResultSet(?)}");
        stmt.setInt(1, 1);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int res = rs.getInt(1);
        assertEquals(res, 1);
    }

    @Test
    public void useParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("a", 1);
        ResultSet rs = stmt.executeQuery();
        rs.next();
        int res = rs.getInt(1);
        assertEquals(res, 1);
    }

    @Test(expected = SQLException.class)
    public void useWrongParameterName() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call useParameterName(?)}");
        stmt.setInt("b", 1);
        fail("must fail");
    }


    @Test
    public void multiResultSets() throws Exception {
        CallableStatement stmt = sharedConnection.prepareCall("{call multiResultSets()}");
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
    public void inoutParam() throws SQLException {
        CallableStatement storedProc = null;


        storedProc = sharedConnection.prepareCall("{call inOutParam(?)}");

        storedProc.setInt(1, 1);
        storedProc.registerOutParameter(1, Types.INTEGER);
        storedProc.execute();
        assertEquals(2, storedProc.getObject(1));
    }

    @Test
    public void getProcedures() throws SQLException {
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "testGetProc%");
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            md.getColumnLabel(i);
        }

        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                rs.getObject(i);
            }
        }
    }

    @Test
    public void withStrangeParameter() throws SQLException {
        CallableStatement stmt = sharedConnection.prepareCall("{call withStrangeParameter(?)}");
        double expected = 5.43;
        stmt.setDouble("a", expected);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        double res = rs.getDouble(1);
        assertEquals(expected, res, 0);
        // now fail due to three decimals
        double tooMuch = 34.987;
        stmt.setDouble("a", tooMuch);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertThat(rs.getDouble(1), is(not(tooMuch)));
        rs.close();
        stmt.close();
    }

    @Test
    public void test1() throws Exception {
        createProcedure("callabletest1", "()\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(null, null, "callabletest1");
        if (rs.next()) {
            assertTrue("callabletest1".equals(rs.getString(3)));
        } else {
            fail();
        }
    }


    @Test
    public void testMetaWildcard() throws Exception {
        createProcedure("testMetaWildcard", "(x int, out y int)\n"
                + "BEGIN\n"
                + "SELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedureColumns(null, null, "testMetaWildcard%", "%");
        if (rs.next()) {
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("x", rs.getString(4));

            assertTrue(rs.next());
            assertEquals("testMetaWildcard", rs.getString(3));
            assertEquals("y", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMetaCatalog() throws Exception {
        createProcedure("testMetaCatalog", "(x int, out y int)\nBEGIN\nSELECT 1;end\n");
        ResultSet rs = sharedConnection.getMetaData().getProcedures(sharedConnection.getCatalog(), null, "testMetaCatalog");
        if (rs.next()) {
            assertTrue("testMetaCatalog".equals(rs.getString(3)));
            assertFalse(rs.next());
        } else {
            fail();
        }
        //test with bad catalog
        rs = sharedConnection.getMetaData().getProcedures("yahoooo", null, "testMetaCatalog");
        assertFalse(rs.next());

        //test without catalog
        rs = sharedConnection.getMetaData().getProcedures(null, null, "testMetaCatalog");
        if (rs.next()) {
            assertTrue("testMetaCatalog".equals(rs.getString(3)));
            assertFalse(rs.next());
        } else {
            fail();
        }
    }

    @Test
    public void testMetaCatalogNoAccessToProcedureBodies() throws Exception {
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
                Assert.assertTrue(sqlException.getMessage().startsWith("Access to metaData informations not granted for current user"));
            }
            callableStatement.setString(1, "1");
            callableStatement.execute();
            try {
                callableStatement.getInt("y");
                fail("Get by named must not succeed");
            } catch (SQLException sqlException) {
                Assert.assertTrue(sqlException.getMessage().startsWith("Access to metaData informations not granted for current user"));
            }
            Assert.assertEquals(2, callableStatement.getInt(2));

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
            callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
            callableStatement.setString(2, "mike");
            callableStatement.execute();
        }
        sharedConnection.setCatalog("testj2");
        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters(?, ?) }")) {
            callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
            callableStatement.setString(2, "mike");
            try {
                callableStatement.execute();
                fail("Should've thrown an exception");
            } catch (SQLException sqlEx) {
                assertEquals("42000", sqlEx.getSQLState());
            }
        }

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call testSameProcedureWithDifferentParameters(?) }")) {
            callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
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
        CallableStatement callableStatement = null;
        try {
            callableStatement = sharedConnection.prepareCall("Call testProcDecimalComa(?)");
            callableStatement.setDouble(1, 18.0);
            callableStatement.execute();
        } finally {
            if (callableStatement != null) {
                callableStatement.close();
            }
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
        java.sql.DatabaseMetaData dbmd = sharedConnection.getMetaData();

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
        rs.next();
        assertEquals("testFunctionCall", rs.getString("PROCEDURE_NAME"));
        assertEquals(java.sql.DatabaseMetaData.procedureReturnsResult, rs.getShort("PROCEDURE_TYPE"));
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
    public void testFunctionWithNoParameters() throws SQLException {
        createFunction("testFunctionWithNoParameters", "()\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN 'mike';");

        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call testFunctionWithNoParameters()}");
        callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
        callableStatement.execute();
        Assert.assertEquals("mike", callableStatement.getString(1));
    }

    @Test
    public void testFunctionWith2parameters() throws SQLException {
        createFunction("testFunctionWith2parameters", "(s CHAR(20), s2 CHAR(20))\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN CONCAT(s,' and ', s2)");

        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call testFunctionWith2parameters(?, ?)}");
        callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
        callableStatement.setString(2, "mike");
        callableStatement.setString(3, "bart");
        callableStatement.execute();
        Assert.assertEquals("mike and bart", callableStatement.getString(1));
    }

    @Test
    public void testFunctionWithFixedParameters() throws SQLException {
        createFunction("testFunctionWith2parameters", "(s CHAR(20), s2 CHAR(20))\n"
                + "    RETURNS CHAR(50) DETERMINISTIC\n"
                + "    RETURN CONCAT(s,' and ', s2)");

        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call testFunctionWith2parameters('mike', ?)}");
        callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
        callableStatement.setString(2, "bart");
        callableStatement.execute();
        Assert.assertEquals("mike and bart", callableStatement.getString(1));
    }

    @Test
    public void testResultsetWithInoutParameter() throws Exception {
        createTable("testResultsetWithInoutParameterTb", "test VARCHAR(10)");
        createProcedure("testResultsetWithInoutParameter", "(INOUT testValue VARCHAR(10))\n"
                + "BEGIN\n"
                + " insert into testResultsetWithInoutParameterTb(test) values (testValue);\n"
                + " SELECT testValue;\n"
                + " SET testValue = UPPER(testValue);\n"
                + "END");
        CallableStatement cstmt = sharedConnection.prepareCall("{call testResultsetWithInoutParameter(?)}");
        cstmt.registerOutParameter(1, java.sql.Types.VARCHAR);
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
        try (Connection connection = setConnection()) {
            createProcedure("simpleproc", "(IN inParam CHAR(20), INOUT inOutParam CHAR(20), OUT outParam CHAR(50))"
                    + "     BEGIN\n"
                    + "         SET inOutParam = UPPER(inOutParam);\n"
                    + "         SET outParam = CONCAT('Hello, ', inOutParam, ' and ', inParam);"
                    + "         SELECT 'a' FROM DUAL;\n"
                    + "     END;");

            final long startTime = System.nanoTime();
            CallableStatement callableStatement = connection.prepareCall("{call simpleproc('mike', ?, ?)}");
            callableStatement.registerOutParameter(1, java.sql.Types.VARCHAR);
            callableStatement.registerOutParameter(2, java.sql.Types.VARCHAR);
            callableStatement.setString(1, "toto");
            callableStatement.execute();
            String result = callableStatement.getString(1);
            String result2 = callableStatement.getString(2);
            if (!"TOTO".equals(result) && !"Hello, TOTO and mike".equals(result2)) {
                Assert.fail();
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

            procDef.append(" OUT param_" + i + " VARCHAR(32)");
            param.append("?");
        }

        procDef.append(")\nBEGIN\nSELECT 1;\nEND");
        createProcedure("testHugeNumberOfParameters", procDef.toString());

        try (CallableStatement callableStatement = sharedConnection.prepareCall(
                "{call testHugeNumberOfParameters(" + param.toString() + ")}") ) {
            callableStatement.registerOutParameter(274, Types.VARCHAR);
            callableStatement.execute();
        }
    }

    @Test
    public void testStreamInOutWithName() throws Exception {
        createProcedure("testStreamInOutWithName", "(INOUT mblob MEDIUMBLOB) BEGIN SELECT 1 FROM DUAL WHERE 1=0;\nEND");
        try (CallableStatement cstmt = sharedConnection.prepareCall("{call testStreamInOutWithName(?)}")) {
            byte[] buffer = new byte[65];
            for (int i = 0; i < 65; i++) {
                buffer[i] = 1;
            }
            int il = buffer.length;
            int[] typesToTest = new int[] { Types.BIT, Types.BINARY, Types.BLOB, Types.JAVA_OBJECT, Types.LONGVARBINARY, Types.VARBINARY };

            for (int i = 0; i < typesToTest.length; i++) {
                cstmt.setBinaryStream("mblob", new ByteArrayInputStream(buffer), buffer.length);
                cstmt.registerOutParameter("mblob", typesToTest[i]);
                cstmt.executeUpdate();

                InputStream is = cstmt.getBlob("mblob").getBinaryStream();
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

                int bytesRead = 0;
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
    }

    @Test
    public void testProcedureComment() throws Exception {
        createProcedure("testProcedureComment", "(a INT, b VARCHAR(32)) BEGIN SELECT CONCAT(CONVERT(a, CHAR(50)), b); END");

        try (CallableStatement callableStatement = sharedConnection.prepareCall("{ call /*comment ? */ testj.testProcedureComment(?, "
                + "/*comment ? */?) #comment ? }")) {
            assertTrue(callableStatement.toString().indexOf("/*") != -1);
            callableStatement.setInt(1, 1);
            callableStatement.setString(2, " a");
            ResultSet rs = callableStatement.executeQuery();
            if (rs.next()) {
                Assert.assertEquals("1 a", rs.getString(1));
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
                new String[] {
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
                        "_RESULT" },
                new int[] { Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR },
                new int[] { 20, 10, 10, 10, 10, 10, 2000, 2000, 8000, 10, 2000, 10 },
                new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
                new int[] { java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnOut,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnIn,
                        java.sql.DatabaseMetaData.procedureColumnOut });

        sharedConnection.prepareCall("{call testCommentParser_1(?, ?)}").close();
        rs = sharedConnection.getMetaData().getProcedureColumns(sharedConnection.getCatalog(), null, "testCommentParser_1", "%");
        validateResult(rs,
                new String[] { "/*id*/", "result2" },
                new int[] { Types.VARCHAR, Types.DECIMAL },
                new int[] { 20, 10 },
                new int[] { 0, 2 },
                new int[] { java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnOut });
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
            }
            assertEquals(scale[index], rs.getInt("SCALE"));
            assertEquals(direction[index], rs.getInt("COLUMN_TYPE"));

            index++;
        }
        rs.close();

    }

    @Test
    public void testCallableNullSetters() throws Throwable {
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

        for (int i = 0; i < setters.length; i++) {
            if (setters[i].getName().startsWith("set")) {
                Class<?>[] args = setters[i].getParameterTypes();

                if (args.length == 2 && args[0].equals(Integer.TYPE)) {
                    if (!args[1].isPrimitive()) {
                        try {
                            setters[i].invoke(callable, new Object[] { new Integer(2), null });
                        } catch (InvocationTargetException ive) {
                            if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                throw ive;
                            }
                        }
                    } else {
                        if (args[1].getName().equals("boolean")) {
                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), Boolean.FALSE });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }
                        }

                        if (args[1].getName().equals("byte")) {

                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), new Byte((byte) 0) });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("double")) {

                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), new Double(0) });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("float")) {

                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), new Float(0) });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("int")) {

                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), new Integer(0) });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }

                        }

                        if (args[1].getName().equals("long")) {
                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), new Long(0) });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }
                        }

                        if (args[1].getName().equals("short")) {
                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), new Short((short) 0) });
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
    public void testBitSp() throws Exception {
        createTable("`Bit_Tab`", "`MAX_VAL` tinyint(1) default NULL, `MIN_VAL` tinyint(1) default NULL, `NULL_VAL` tinyint(1) default NULL");
        createProcedure("Bit_Proc", "(out MAX_PARAM TINYINT, out MIN_PARAM TINYINT, out NULL_PARAM TINYINT)"
                + "begin select MAX_VAL, MIN_VAL, NULL_VAL  into MAX_PARAM, MIN_PARAM, NULL_PARAM from Bit_Tab; end");

        sharedConnection.createStatement().executeUpdate("delete from Bit_Tab");
        sharedConnection.createStatement().executeUpdate("insert into Bit_Tab values(1,0,null)");
        CallableStatement callableStatement = sharedConnection.prepareCall("{call Bit_Proc(?,?,?)}");

        System.out.println("register the output parameters");
        callableStatement.registerOutParameter(1, java.sql.Types.BIT);
        callableStatement.registerOutParameter(2, java.sql.Types.BIT);
        callableStatement.registerOutParameter(3, java.sql.Types.BIT);

        System.out.println("execute the procedure");
        callableStatement.executeUpdate();

        System.out.println("invoke getBoolean method");
        Boolean returnValue = new Boolean(callableStatement.getBoolean(2));
        Boolean minBooleanVal = new Boolean("false");
        ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT MIN_VAL from Bit_Tab");
        if (returnValue.equals(minBooleanVal)) {
            System.out.println("getBoolean returns the Minimum value ");
        } else {
            System.out.println("getBoolean() did not return the Minimum value, getBoolean Failed!");

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
                + "\nOUT fdoc VARCHAR(100))\nBEGIN\nSET nfact = 'ncfact string';\nSET ffact = 'ffact string';\nSET fdoc = 'fdoc string';\nEND");

        Properties props = new Properties();
        props.put("jdbcCompliantTruncation", "true");
        props.put("useInformationSchema", "true");
        Connection conn1 = null;
        conn1 = setConnection(props);
        try {
            CallableStatement callSt = conn1.prepareCall("{ call testParameterNumber_1(?, ?, ?, ?) }");
            callSt.setString(2, "xxx");
            callSt.registerOutParameter(1, java.sql.Types.VARCHAR);
            callSt.registerOutParameter(3, java.sql.Types.VARCHAR);
            callSt.registerOutParameter(4, java.sql.Types.VARCHAR);
            callSt.execute();

            assertEquals("ncfact string", callSt.getString(1));
            assertEquals("ffact string", callSt.getString(3));
            assertEquals("fdoc string", callSt.getString(4));

            CallableStatement callSt2 = conn1.prepareCall("{ call testParameterNumber_2(?, ?, ?, ?, ?) }");
            callSt2.setString(1, "xxx");
            callSt2.setString(2, "yyy");
            callSt2.registerOutParameter(3, java.sql.Types.VARCHAR);
            callSt2.registerOutParameter(4, java.sql.Types.VARCHAR);
            callSt2.registerOutParameter(5, java.sql.Types.VARCHAR);
            callSt2.execute();

            assertEquals("ncfact string", callSt2.getString(3));
            assertEquals("ffact string", callSt2.getString(4));
            assertEquals("fdoc string", callSt2.getString(5));

            CallableStatement callSt3 = conn1.prepareCall("{ call testParameterNumber_2(?, 'yyy', ?, ?, ?) }");
            callSt3.setString(1, "xxx");
            // callSt3.setString(2, "yyy");
            callSt3.registerOutParameter(2, java.sql.Types.VARCHAR);
            callSt3.registerOutParameter(3, java.sql.Types.VARCHAR);
            callSt3.registerOutParameter(4, java.sql.Types.VARCHAR);
            callSt3.execute();

            assertEquals("ncfact string", callSt3.getString(2));
            assertEquals("ffact string", callSt3.getString(3));
            assertEquals("fdoc string", callSt3.getString(4));
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testProcMultiDb() throws Exception {
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
            callableStatement.clearParameters();
            callableStatement.close();
            sharedConnection.setCatalog(originalCatalog);
            sharedConnection.createStatement().executeUpdate("DROP DATABASE testProcMultiDb");
        }

    }


    @Test
    public void testProcSendNullInOut() throws Exception {
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
        createFunction("hello", "()\n" +
                "    RETURNS CHAR(50) DETERMINISTIC\n" +
                "    RETURN CONCAT('Hello, !');");
        CallableStatement callableStatement = sharedConnection.prepareCall("{? = call `hello` ()}");
        callableStatement.registerOutParameter(1, Types.INTEGER);
        assertFalse(callableStatement.execute());
        assertEquals("Hello, !", callableStatement.getString(1));
    }

}
