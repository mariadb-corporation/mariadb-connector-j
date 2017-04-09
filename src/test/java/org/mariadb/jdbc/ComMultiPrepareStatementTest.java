package org.mariadb.jdbc;

import java.sql.PreparedStatement;

public class ComMultiPrepareStatementTest extends BaseTest {


    @org.junit.Test
    public void insertSelectTempTable2() throws Exception {
        requireMinimumVersion(10, 2);
        createTable("test_insert_select_com_multi", "`field1` varchar(20)");
        //prepare doesn't work.
        PreparedStatement stmt = sharedConnection.prepareStatement(
                "select  TMP.field1 from (select CAST(? as binary) `field1` from dual) TMP");
        stmt.setString(1, "test");
        stmt.executeUpdate();
    }

}
