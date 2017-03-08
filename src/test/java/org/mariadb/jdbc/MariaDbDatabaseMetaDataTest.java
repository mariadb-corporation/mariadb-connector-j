package org.mariadb.jdbc;

import org.junit.Test;
import java.sql.*;

import static org.junit.Assert.*;

public class MariaDbDatabaseMetaDataTest extends BaseTest {

    /**
     * CONJ-412: tinyInt1isBit and yearIsDateType is not applied in method columnTypeClause.
     * @throws Exception if exception occur
     */
    @Test
    public void testYearDataType() throws Exception {
        createTable("yearTableMeta", "xx tinyint(1), yy year(4), zz bit, uu smallint");
        try (Connection connection = setConnection()) {
            checkResults(connection, true, true);
        }

        try (Connection connection = setConnection("&yearIsDateType=false&tinyInt1isBit=false")) {
            checkResults(connection, false, false);
        }
    }

    private void checkResults(Connection connection, boolean yearAsDate, boolean tinyAsBit) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet rs = meta.getColumns(null, null, "yearTableMeta", null);
        assertTrue(rs.next());
        assertEquals(tinyAsBit ? "BIT" : "TINYINT", rs.getString(6));
        assertTrue(rs.next());
        assertEquals(yearAsDate ? "YEAR" : "SMALLINT" , rs.getString(6));
        assertEquals(yearAsDate ? null : "5" , rs.getString(7)); // column size
        assertEquals(yearAsDate ? null : "0", rs.getString(9)); // decimal digit

    }

}