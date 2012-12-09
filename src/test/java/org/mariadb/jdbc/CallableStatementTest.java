    package org.mariadb.jdbc;


    import org.junit.Test;

import java.sql.*;

    import static junit.framework.Assert.*;

    public class CallableStatementTest extends BaseTest{


        @Test
        public void CallSimple()throws SQLException {
            CallableStatement st = connection.prepareCall("{?=call pow(?,?)}");
            st.setInt(2,2);
            st.setInt(3,2);
            st.execute();
            int result = st.getInt(1);
            assertEquals(result, 4);

        }

        private void create(String objType, String name, String body) throws SQLException{
            Statement st = connection.createStatement();
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

        @Test
        public void withResultSet() throws Exception {
            createProcedure("withResultSet", "(a int) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call withResultSet(?)}");
            stmt.setInt(1,1);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int res = rs.getInt(1);
            assertEquals(res, 1);
        }

        @Test
        public void useParameterName() throws Exception {
            createProcedure("useParameterName", "(a int) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call useParameterName(?)}");
            stmt.setInt("a",1);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int res = rs.getInt(1);
            assertEquals(res, 1);
        }

        @Test
        public void useWrongParameterName() throws Exception {
            createProcedure("useWrongParameterName", "(a int) begin select a; end");
            CallableStatement stmt = connection.prepareCall("{call useParameterName(?)}");

            try {
                stmt.setInt("b",1);
                fail("must fail");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage().equals("there is no parameter with the name b"));
            }
        }
        @Test
        public void multiResultSets() throws Exception {
            createProcedure("multiResultSets", "() BEGIN  SELECT 1; SELECT 2; END");
            CallableStatement stmt = connection.prepareCall("{call multiResultSets()}");
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
        public void inoutParam() throws SQLException{
            CallableStatement storedProc = null;

            createProcedure("inoutParam",
                    "(INOUT p1 INT) begin set p1 = p1 + 1; end\n");

            storedProc = connection.prepareCall("{call inOutParam(?)}");

            storedProc.setInt(1,1);
            storedProc.registerOutParameter(1, Types.INTEGER);
            storedProc.execute();
            assertEquals(2, storedProc.getObject(1));

        }
    }
