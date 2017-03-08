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
        assertFalse(checkRewritable("SELECT * FROM MyTable"));
        assertFalse(checkRewritable("SELECT\n * FROM MyTable"));
        assertFalse(checkRewritable("SELECT(1)"));
        assertFalse(checkRewritable("INSERT MyTable (a) VALUES (1);SELECT(1)"));
    }

    /**
     * INSERT FROM SELECT are not be rewritable.
     */
    @Test
    public void insertSelectQuery() {
        assertFalse(checkRewritable("INSERT INTO MyTable (a) SELECT * FROM seq_1_to_1000"));
        assertFalse(checkRewritable("INSERT INTO MyTable (a);SELECT * FROM seq_1_to_1000"));
        assertFalse(checkRewritable("INSERT INTO MyTable (a)SELECT * FROM seq_1_to_1000"));
        assertFalse(checkRewritable("INSERT INTO MyTable (a) (SELECT * FROM seq_1_to_1000)"));
        assertFalse(checkRewritable("INSERT INTO MyTable (a) SELECT\n * FROM seq_1_to_1000"));
    }

    /**
     * Insert query that contain table/column name with select keyword, or select in comment can be rewritten.
     */
    @Test
    public void rewritableThatContainSelectQuery() {
        //but 'SELECT' keyword in column/table name can be rewritable
        assertTrue(checkRewritable("INSERT INTO TABLE_SELECT "));
        assertTrue(checkRewritable("INSERT INTO TABLE_SELECT"));
        assertTrue(checkRewritable("INSERT INTO SELECT_TABLE"));
        assertTrue(checkRewritable("INSERT INTO `TABLE SELECT `"));
        assertTrue(checkRewritable("INSERT INTO TABLE /* SELECT in comment */ "));
        assertTrue(checkRewritable("INSERT INTO TABLE //SELECT"));
    }

    private boolean checkRewritable(String query) {
        return ClientPrepareResult.rewritableParts(query, true).isQueryMultiValuesRewritable();
    }
}
