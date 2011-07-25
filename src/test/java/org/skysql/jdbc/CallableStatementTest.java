    package org.skysql.jdbc;


    import org.junit.Test;

    import java.sql.*;

    import static junit.framework.Assert.assertEquals;
    import static junit.framework.Assert.assertTrue;

    public class CallableStatementTest {
        Connection con;
        public CallableStatementTest() throws SQLException{
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test");
        }

        @Test
        public void CallSimple()throws SQLException {
            CallableStatement st = con.prepareCall("{?=call pow(?,?)}");
            st.setInt(2,2);
            st.setInt(3,2);
            st.execute();
            int result = st.getInt(1);
            assertEquals(result, 4);

        }

        private void create(String objType, String name, String body) throws SQLException{
            Statement st = con.createStatement();
            try {
                st.execute("drop " + objType + " " + name);
            }
            catch(Exception e) {
                // eat exception
            }
            st.execute("create  "+ objType + " " + name + body);
        }
        private void createProcedure(String name, String body) throws SQLException{
            create("procedure", name, body);
        }

        private void createTable(String name, String body) throws SQLException {
            create("table", name, body);
        }

        private void createFunction(String name, String body) throws SQLException {
            create("function", name , body) ;
        }
        @Test
        public void testInOut() throws SQLException{
            CallableStatement storedProc = null;

            createProcedure("testInOutParam",
                    "(IN p1 VARCHAR(255), INOUT p2 INT)\n" + "begin\n"
                            + " DECLARE z INT;\n" + "SET z = p2 + 1;\n"
                            + "SET p2 = z;\n" + "SELECT p1;\n"
                            + "SELECT CONCAT('zyxw', p1);\n" + "end\n");

            storedProc = con.prepareCall("{call testInOutParam(?, ?)}");

            storedProc.setString(1, "abcd");
            storedProc.setInt(2, 4);
            storedProc.registerOutParameter(2, Types.VARCHAR);
            storedProc.execute();
            Object o = storedProc.getObject("p2");
            System.out.println("object = " + o + ":" + o.getClass());
            assertEquals(5, storedProc.getInt("p2"));
            //ParameterMetaData md= storedProc.getParameterMetaData();
        }

        @Test
        public void testParameterParser() throws Exception {

            CallableStatement cstmt = null;

            try {

                createTable("t1",
                        "(id   char(16) not null default '', data int not null)");
                createTable("t2", "(s   char(16),  i   int,  d   double)");

                createProcedure("foo42",
                        "() insert into test.t1 values ('foo', 42);");
                PreparedStatement s = this.con.prepareCall("{CALL foo42()}");
                assertEquals(s.getParameterMetaData().getParameterCount(), 0);

                this.con.prepareCall("{CALL foo42}");
                assertEquals(s.getParameterMetaData().getParameterCount(), 0);


                createProcedure("bar",
                        "(x char(16), y int, z DECIMAL(10)) insert into test.t1 values (x, y);");
                cstmt = this.con.prepareCall("{CALL bar(?, ?, ?)}");

                ParameterMetaData md = cstmt.getParameterMetaData();
                assertEquals(3, md.getParameterCount());
                assertEquals(Types.CHAR, md.getParameterType(1));
                assertEquals(Types.INTEGER, md.getParameterType(2));
                assertEquals(Types.DECIMAL, md.getParameterType(3));

                createProcedure("p", "() label1: WHILE @a=0 DO SET @a=1; END WHILE");
                this.con.prepareCall("{CALL p()}");

                createFunction("f", "() RETURNS INT NO SQL return 1; ");
                cstmt = this.con.prepareCall("{? = CALL f()}");

                md = cstmt.getParameterMetaData();
                assertEquals(Types.INTEGER, md.getParameterType(1));

            } finally {
                if (cstmt != null) {
                    cstmt.close();
                }
            }
        }

        @Test
        public void testBatch() throws SQLException{
            createTable("testBatchTable", "(field1 INT)");
            createProcedure("testBatch", "(IN foo VARCHAR(15))\n"
                            + "begin\n"
                            + "INSERT INTO testBatchTable VALUES (foo);\n"
                            + "end\n");
            CallableStatement st = con.prepareCall("{call testBatch(?)}");
            try {
                for(int i=0; i< 100; i++) {
                    st.setInt(1, i);
                    st.addBatch();
                }
                int[] a = st.executeBatch();
                assertEquals(a.length,100);
                for(int i=0; i< 100; i++)
                    assertEquals(a[i],1);
            }
            finally {
                st.close();
            }

            Statement s = con.createStatement();
            ResultSet rs = st.executeQuery("select * from testBatchTable");
            for(int i=0; i< 100; i++) {
                assertTrue(rs.next());
                assertEquals(i,rs.getInt(1));
            }
        }

        @Test
        public void testOutParamsNoBodies() throws Exception {

                CallableStatement storedProc = null;


                createProcedure("testOutParam", "(x int, out y int)\n" + "begin\n"
                        + "declare z int;\n" + "set z = x+1, y = z;\n" + "end\n");

                storedProc = con.prepareCall("{call testOutParam(?, ?)}");

                storedProc.setInt(1, 5);
                storedProc.registerOutParameter(2, Types.INTEGER);

                storedProc.execute();

                int indexedOutParamToTest = storedProc.getInt(2);

                assertTrue("Output value not returned correctly",
                        indexedOutParamToTest == 6);

                storedProc.clearParameters();
                storedProc.setInt(1, 32);
                storedProc.registerOutParameter(2, Types.INTEGER);

                storedProc.execute();

                indexedOutParamToTest = storedProc.getInt(2);

                assertTrue("Output value not returned correctly",
                        indexedOutParamToTest == 33);
        }


        @Test
        public void testSPNoParams() throws Exception {
                CallableStatement storedProc = null;
                createProcedure("testSPNoParams", "()\n" + "BEGIN\n"
                        + "SELECT 1;\n" + "end\n");
                storedProc = con.prepareCall("{call testSPNoParams()}");
                storedProc.execute();
        }

        @Test
        public void testResultSet() throws Exception {

                CallableStatement storedProc = null;

                createTable("testSpResultTbl1", "(field1 INT)");
                Statement stmt = con.createStatement();
                stmt.executeUpdate("INSERT INTO testSpResultTbl1 VALUES (1), (2)");
                createTable("testSpResultTbl2", "(field2 varchar(255))");
                stmt.executeUpdate("INSERT INTO testSpResultTbl2 VALUES ('abc'), ('def')");
                stmt.close();

                createProcedure(
                        "testSpResult",
                        "()\n"
                                + "BEGIN\n"
                                + "SELECT field2 FROM testSpResultTbl2 WHERE field2='abc';\n"
                                + "UPDATE testSpResultTbl1 SET field1=2;\n"
                                + "SELECT field2 FROM testSpResultTbl2 WHERE field2='def';\n"
                                + "end\n");

                storedProc = con.prepareCall("{call testSpResult()}");

                storedProc.execute();

                ResultSet rs = storedProc.getResultSet();

                ResultSetMetaData rsmd = rs.getMetaData();

                assertTrue(rsmd.getColumnCount() == 1);
                assertTrue("field2".equals(rsmd.getColumnName(1)));
                assertTrue(rsmd.getColumnType(1) == Types.VARCHAR);

                assertTrue(rs.next());

                assertTrue("abc".equals(rs.getString(1)));

                assertTrue(storedProc.getMoreResults());

                ResultSet nextResultSet = storedProc.getResultSet();

                rsmd = nextResultSet.getMetaData();

                assertTrue(rsmd.getColumnCount() == 1);
                assertTrue("field2".equals(rsmd.getColumnName(1)));
                assertTrue(rsmd.getColumnType(1) == Types.VARCHAR);

                assertTrue(nextResultSet.next());

                assertTrue("def".equals(nextResultSet.getString(1)));

                nextResultSet.close();

                rs.close();

                storedProc.execute();
        }

        public void testOutParams() throws Exception {
                CallableStatement storedProc = null;

                createProcedure("testOutParam", "(x int, out y int)\n" + "begin\n"
                        + "declare z int;\n" + "set z = x+1, y = z;\n" + "end\n");

                storedProc = con.prepareCall("{call testOutParam(?, ?)}");

                storedProc.setInt(1, 5);
                storedProc.registerOutParameter(2, Types.INTEGER);

                storedProc.execute();

                System.out.println(storedProc);

                int indexedOutParamToTest = storedProc.getInt(2);


                int namedOutParamToTest = storedProc.getInt("y");

                assertTrue("Named and indexed parameter are not the same",
                        indexedOutParamToTest == namedOutParamToTest);
                assertTrue("Output value not returned correctly",
                        indexedOutParamToTest == 6);

                // Start over, using named parameters, this time
                storedProc.clearParameters();
                storedProc.setInt("x", 32);
                storedProc.registerOutParameter("y", Types.INTEGER);

                storedProc.execute();

                indexedOutParamToTest = storedProc.getInt(2);
                namedOutParamToTest = storedProc.getInt("y");

                assertTrue("Named and indexed parameter are not the same",
                        indexedOutParamToTest == namedOutParamToTest);
                assertTrue("Output value not returned correctly",
                        indexedOutParamToTest == 33);
            }

    }
