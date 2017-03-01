package org.mariadb.jdbc.internal.util.dao;

import org.mariadb.jdbc.BaseTest;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ClientPrepareResultTest extends BaseTest {

    @Test
    public void testRewritablePartsQueryWithSelectInColumnName() {
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO EXAMPLE (COLUMN1, SELECT_COLUMN2) ", false).isQueryMultiValuesRewritable());
    }

    @Test
    public void testRewritablePartsQueryWithUnderScoreSelectInColumnName() {
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO EXAMPLE (COLUMN1, _SELECT_COLUMN2) ", false).isQueryMultiValuesRewritable());
    }

    @Test
    public void testRewritablePartsQueryWithEscapedSelectInColumnName() {
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO EXAMPLE (`COLUMN1`, `SELECT_COLUMN2`) ", false).isQueryMultiValuesRewritable());
    }


    @Test
    public void testRewritablePartsQueryWithSelect() {
        assertFalse(ClientPrepareResult.rewritableParts("SELECT * FROM EXAMPLE", false).isQueryMultiValuesRewritable());
    }

    @Test
    public void testRewritablePartsQueryWithValuesInColumnName() {
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO EXAMPLE (COLUMN1, VALUES_COLUMN2)",
                    false).isQueryMultiValuesRewritable());
    }

    @Test
    public void testRewritablePartsQueryWithUnderScoreValuesInColunmName() {
        assertTrue(ClientPrepareResult.rewritableParts("INSERT INTO EXAMPLE (COLUMN1, _VALUES_COLUMN2)",
                    false).isQueryMultiValuesRewritable());
    }
}
