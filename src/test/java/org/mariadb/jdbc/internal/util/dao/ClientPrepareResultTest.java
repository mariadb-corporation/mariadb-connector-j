package org.mariadb.jdbc.internal.util.dao;

import static org.junit.Assert.*;

import org.junit.Test;

public class ClientPrepareResultTest {

    /**
     * SELECT query cannot be rewritable.
     */
    @Test
    public void selectQuery() {
        //SELECT query cannot be rewritable
        assertFalse(ClientPrepareResult.rewritableParts("SELECT * FROM MyTable", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("SELECT\n * FROM MyTable", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("SELECT(1)", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("INSERT MyTable (a) VALUES (1);SELECT(1)", true).isQueryMultiValuesRewritable());
    }

    /**
     * INSERT FROM SELECT are not be rewritable.
     */
    @Test
    public void insertSelectQuery() {
        assertFalse(ClientPrepareResult.rewritableParts("INSERT INTO MyTable (a) SELECT * FROM seq_1_to_1000", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("INSERT INTO MyTable (a);SELECT * FROM seq_1_to_1000", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("INSERT INTO MyTable (a)SELECT * FROM seq_1_to_1000", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("INSERT INTO MyTable (a) (SELECT * FROM seq_1_to_1000)", true).isQueryMultiValuesRewritable());
        assertFalse(ClientPrepareResult.rewritableParts("INSERT INTO MyTable (a) SELECT\n * FROM seq_1_to_1000", true).isQueryMultiValuesRewritable());
    }

    /**
     * Insert query that contain table/column name with select keyword, or select in comment can be rewritten
     */
    @Test
    public void rewritableThatContainSelectQuery() {
        //but 'SELECT' keyword in column/table name can be rewritable
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO TABLE_SELECT ", true).isQueryMultiValuesRewritable());
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO TABLE_SELECT", true).isQueryMultiValuesRewritable());
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO SELECT_TABLE", true).isQueryMultiValuesRewritable());
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO `TABLE SELECT `", true).isQueryMultiValuesRewritable());
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO TABLE /* SELECT in comment */ ", true).isQueryMultiValuesRewritable());
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO TABLE //SELECT", true).isQueryMultiValuesRewritable());

    }
}
