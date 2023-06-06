package org.mariadb.jdbc.integration;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Statement;

public class BulkStmtSplitErrorTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS BulkStmtSplitError");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE BulkStmtSplitError (t1 int not null primary key, field varchar(300))");
    stmt.execute("FLUSH TABLES");
  }

  @Test
  public void BulkStmtSplitError() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO BulkStmtSplitError VALUES (1, 'tt'), (2, 'tt2')");

    try (PreparedStatement prep =
        sharedConn.prepareStatement("insert into BulkStmtSplitError values (?, ?)")) {
      prep.setInt(1, 1);
      prep.setNull(2, Types.VARCHAR);
      prep.addBatch();
      prep.setInt(1, 2);
      prep.setString(2, "Kenny");
      prep.addBatch();
      prep.executeBatch();
    } catch (SQLException e) {
      // eat
    }

    ResultSet rs = stmt.executeQuery("SELECT count(*) FROM BulkStmtSplitError");
    Assertions.assertTrue(rs.next());
    Assertions.assertEquals(2, rs.getInt(1));
  }
}
