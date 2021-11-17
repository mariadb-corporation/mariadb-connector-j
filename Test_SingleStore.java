/*************************************************************************************
 Copyright (c) 2021 SingleStore, Inc.

 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Library General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.

 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Library General Public License for more details.

 You should have received a copy of the GNU Library General Public
 License along with this library; if not see <http://www.gnu.org/licenses>
 or write to the Free Software Foundation, Inc.,
 51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *************************************************************************************/

// Temporary test to run on CI while we are fixing the actual tests

import java.sql.*;
import java.util.Properties;

public class Test_SingleStore
{
    // The JDBC Connector Class.
    private static final String dbClassName = "com.singlestore.jdbc.Driver";

    private static final String CONNECTION = "jdbc:singlestore://127.0.0.1:5506/";
    private static final String ROOT_PASSWORD = "password";

    public static void ASSERT(boolean cond)
    {
        if (!cond)
        {
            throw new RuntimeException("ASSERT FAILED");
        }
    }

    public static void ResetEnvironment() throws SQLException
    {
        {
            Properties p = new Properties();
            p.put("user", "root");
            p.put("password", ROOT_PASSWORD);

            Connection conn = DriverManager.getConnection(CONNECTION, p);

            for (String query : new String[]{
                    "DROP DATABASE IF EXISTS test1"
                    , "DROP DATABASE IF EXISTS test2"
                    , "CREATE DATABASE test1"
                    , "CREATE DATABASE test2"
                    , "GRANT ALL ON test1.* TO 'test'"
                    , "GRANT CLUSTER ON *.* TO 'test'"
                    , "USE test1"
                    , "CREATE TABLE x (id int primary key auto_increment, a int default 0)"
                    , "INSERT INTO x (id) VALUES (1), (2), (3)"
                    , "CREATE TABLE y (id int, a char(5), b as substr(a, 4, 1) persisted char(4))"
            })
            {
                Statement stmt = conn.createStatement();
                stmt.execute(query);
                stmt.close();
            }

            conn.close();
        }
    }

    public static void RunTest(String databaseName, String user,
                               String password, boolean expectLoginToWork) throws SQLException
    {
        ResetEnvironment();
        Connection conn = null;

        String conStr = "jdbc:singlestore://localhost:5506/test1?user=" + user + "&password=" + password;

        try {
            conn = DriverManager.getConnection(conStr);
        } catch (SQLException e) {
            if (expectLoginToWork) {
                throw e;
            } else {
                return;
            }
        }

        conn.getMetaData();

        // These are examples from the JDBC documentation:
        // http://dev.mysql.com/doc/connector-j/en/connector-j-examples.html
        // http://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html
        //
        Statement stmt = null;
        ResultSet rs = null;

        stmt = conn.createStatement();
        stmt.execute("USE test1");
        stmt.close();

        // Run a basic SELECT query
        //
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM x");

        int resultSum = 0;
        while (rs.next())
        {
            resultSum += rs.getInt("id");
        }
        ASSERT(resultSum == 6);
        stmt.close();
        {
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM x where ?");
            ps.setInt(1, 1);
            rs = ps.executeQuery();

            resultSum = 0;
            while (rs.next())
            {
                resultSum += rs.getInt("id");
            }
            ASSERT(resultSum == 6);
            ps.close();
        }
        {
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM x where id = ?");
            ps.setInt(1, 2);
            rs = ps.executeQuery();

            resultSum = 0;
            while (rs.next())
            {
                resultSum += rs.getInt("id");
            }
            ASSERT(resultSum == 2);
            ps.close();
        }

        // Try doing parameter substitution for an INSERT query
        //
        PreparedStatement insertRow = conn.prepareStatement("INSERT INTO x (id) VALUES (?), (?)");
        insertRow.setInt(1, 4);
        insertRow.setString(2, "5");
        insertRow.executeUpdate();
        insertRow.close();

        // Check the results of the INSERT
        //
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM x");

        resultSum = 0;
        while (rs.next())
        {
            resultSum += rs.getInt("id");
        }
        ASSERT(resultSum == 15);
        stmt.close();

        try {
            stmt = conn.createStatement();
            stmt.execute("aggregator sync auto_increment");
            stmt.close();
        } catch (SQLException e)
        {
            // fails in singlebox
        }


        // Try retrieving the AUTO_INCREMENT value
        //
        stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO x (a) VALUES (2)", Statement.RETURN_GENERATED_KEYS);

        rs = stmt.getGeneratedKeys();
        ASSERT(rs.next());
        ASSERT(rs.getInt(1) == 6);
        ASSERT(!rs.next());
        stmt.close();


        // Try using SELECT LAST_INSERT_ID()
        //
        stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO x (a) VALUES (2)");
        rs = stmt.executeQuery("SELECT LAST_INSERT_ID()");
        ASSERT(rs.next());
        ASSERT(rs.getInt(1) == 7);
        ASSERT(!rs.next());

        try
        {
            Savepoint sp = conn.setSavepoint();
            // should fail
            ASSERT(false);
        } catch (SQLFeatureNotSupportedException ignored) {

        }

        conn.getTransactionIsolation();
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

        conn.setCatalog("test1");

        // Bunch of methods from the API doc
        // http://docs.oracle.com/javase/7/docs/api/java/sql/Connection.html
        //
        conn.clearWarnings();
        conn.getAutoCommit();
        conn.getHoldability();
        conn.getCatalog();
        ASSERT(!conn.isClosed());
        ASSERT(!conn.isReadOnly());

        // UNDONE: test transactions (http://dev.mysql.com/doc/connector-j/en/connector-j-usagenotes-troubleshooting.html#connector-j-examples-transaction-retry)
        // properly once they land
        //
        conn.setAutoCommit(false);
        stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO x VALUES (8, 1)");
        conn.rollback();
        stmt.close();

        // This will fail when transactions land
        //
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT * FROM x WHERE id = 7");
        ASSERT(rs.next());
        ASSERT(!rs.next());
        stmt.close();

        // Test some Statement methods
        //
        stmt = conn.createStatement();
        stmt.cancel();

        stmt = conn.createStatement();
        stmt.clearBatch();

        stmt = conn.createStatement();
        stmt.addBatch("INSERT INTO x (a) VALUES (4)");
        stmt.addBatch("INSERT INTO x (a) VALUES (5)");
        stmt.executeBatch();

        rs = stmt.executeQuery("SELECT COUNT(*) as c FROM x WHERE a >= 4");
        ASSERT(rs.next());
        ASSERT(rs.getInt("c") == 2);

        stmt.getFetchSize();

        stmt.executeUpdate("GENERATE WARNINGS 5");
        stmt.getWarnings();

        stmt.setMaxRows(1);
        rs = stmt.executeQuery("SELECT * FROM x ORDER BY id");
        ASSERT(rs.next());
        ASSERT(rs.getInt("id") == 1);

        // We don't actually support this properly in the engine (it's SQL_SELECT_LIMIT)
        // so we can't assert that rs.next() is false
        while(rs.next()) ;

        // play with DatabaseMetaData
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet res1 = meta.getColumns(null, null, "x", null);
        ResultSet res2 = meta.getColumns("", "", "x", "");
        while (res1.next() && res2.next())
        {
            ASSERT(res1.getString("TABLE_SCHEM") == null && res2.getString("TABLE_SCHEM") == null || res1.getString("TABLE_SCHEM").equals(res2.getString("TABLE_SCHEM")));
            ASSERT(res1.getString("TABLE_NAME").equals(res2.getString("TABLE_NAME")));
            ASSERT(res1.getString("COLUMN_NAME").equals(res2.getString("COLUMN_NAME")));
            ASSERT(res1.getString("TYPE_NAME").equals(res2.getString("TYPE_NAME")));
            ASSERT(res1.getInt("COLUMN_SIZE") == res2.getInt("COLUMN_SIZE"));
            ASSERT(res1.getString("NULLABLE").equals(res2.getString("NULLABLE")));
        }
        res1.close();
        res2.close();

        try
        {
            res1 = meta.getImportedKeys("","","x");
            // should fail
            ASSERT(false);
        } catch (SQLFeatureNotSupportedException ignored) {

        }
        // ASSERT(res1.next());
        res2 = meta.getPrimaryKeys("","","x");
        // tables are columnstore by default starting from 7.5 and primary keys are not displayed correctly there
        if (meta.getDatabaseMajorVersion() < 7 ||
                (meta.getDatabaseMajorVersion() == 7 && meta.getDatabaseMinorVersion() < 5)) {
            ASSERT(res2.next());
        }

        // Make sure computed columns work with getColumns
        //
        stmt = conn.createStatement();
        meta = conn.getMetaData();
        ResultSet columns = meta.getColumns(null, null, "y", null);

        int colCount = 0;
        while (columns.next()) {
            colCount++;
        }
        ASSERT(colCount == 3);

        conn.close();
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException
    {
        Class.forName(dbClassName);

        ResetEnvironment();
        for (String contextDb : new String[]{"", "test", "test1", "test2"})
        {
            RunTest(contextDb, "root", "", false);
            RunTest(contextDb, "root", ROOT_PASSWORD, true);
            RunTest(contextDb, "test", "", true);
            RunTest(contextDb, "test", "test_pass", false);
        }
        ResetEnvironment();
    }
}
