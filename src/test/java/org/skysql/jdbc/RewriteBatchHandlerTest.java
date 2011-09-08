package org.skysql.jdbc;

import org.junit.Test;
import org.skysql.jdbc.internal.common.*;
import org.skysql.jdbc.internal.common.query.MySQLQuery;
import org.skysql.jdbc.internal.common.query.ParameterizedQuery;
import org.skysql.jdbc.internal.common.query.MySQLParameterizedQuery;
import org.skysql.jdbc.internal.common.query.IllegalParameterException;
import org.skysql.jdbc.internal.common.query.parameters.StringParameter;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static junit.framework.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jul 17, 2009
 * Time: 3:46:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class RewriteBatchHandlerTest {
    @Test
    public void testRewriteBatchHanderMockProtocol() throws QueryException, IllegalParameterException {
        RewriteParameterizedBatchHandlerFactory rpbhf = new RewriteParameterizedBatchHandlerFactory();
        Protocol mockProtocol = mock(Protocol.class);
        String query = "insert into abc values (?,?,?)";
        ParameterizedBatchHandler rpbh = rpbhf.get(query, mockProtocol);
        assertTrue(rpbh instanceof RewriteParameterizedBatchHandler);
        for(int i = 0;i<3;i++) {
            ParameterizedQuery pq = new MySQLParameterizedQuery(query);
            pq.setParameter(0, new StringParameter("a"+i, false));
            pq.setParameter(1, new StringParameter("b"+i, false));
            pq.setParameter(2, new StringParameter("c"+i, false));
            rpbh.addToBatch(pq);
        }
        rpbh.executeBatch();
        String expectedQuery = "insert into abc values('a0','b0','c0'),('a1','b1','c1'),('a2','b2','c2')";
        verify(mockProtocol).executeQuery(new MySQLQuery(expectedQuery));
    }
    @Test
    public void testWithColumns() throws QueryException, IllegalParameterException {
        RewriteParameterizedBatchHandlerFactory rpbhf = new RewriteParameterizedBatchHandlerFactory();
        Protocol mockProtocol = mock(Protocol.class);
        String query = "insert into abc (c1,c2,c3) values (?,?,?)";
        ParameterizedBatchHandler rpbh = rpbhf.get(query, mockProtocol);
        assertTrue(rpbh instanceof RewriteParameterizedBatchHandler);
        for(int i = 0;i<3;i++) {
            ParameterizedQuery pq = new MySQLParameterizedQuery(query);
            pq.setParameter(0, new StringParameter("a"+i, false));
            pq.setParameter(1, new StringParameter("b"+i, false));
            pq.setParameter(2, new StringParameter("c"+i, false));
            rpbh.addToBatch(pq);
        }
        rpbh.executeBatch();
        String expectedQuery = "insert into abc (c1,c2,c3) values('a0','b0','c0'),('a1','b1','c1'),('a2','b2','c2')";
        verify(mockProtocol).executeQuery(new MySQLQuery(expectedQuery));
    }
    @Test
    public void testWithoutInto() throws QueryException, IllegalParameterException {
        RewriteParameterizedBatchHandlerFactory rpbhf = new RewriteParameterizedBatchHandlerFactory();
        Protocol mockProtocol = mock(Protocol.class);
        String query = "insert abc (c1,c2,c3) value (?,?,?)";
        ParameterizedBatchHandler rpbh = rpbhf.get(query, mockProtocol);
        assertTrue(rpbh instanceof RewriteParameterizedBatchHandler);
        for(int i = 0;i<3;i++) {
            ParameterizedQuery pq = new MySQLParameterizedQuery(query);
            pq.setParameter(0, new StringParameter("a"+i, false));
            pq.setParameter(1, new StringParameter("b"+i, false));
            pq.setParameter(2, new StringParameter("c"+i, false));
            rpbh.addToBatch(pq);
        }
        rpbh.executeBatch();
        String expectedQuery = "insert abc (c1,c2,c3) value('a0','b0','c0'),('a1','b1','c1'),('a2','b2','c2')";
        verify(mockProtocol).executeQuery(new MySQLQuery(expectedQuery));
    }
    @Test
    public void testOnDupKey() throws QueryException, IllegalParameterException {
        RewriteParameterizedBatchHandlerFactory rpbhf = new RewriteParameterizedBatchHandlerFactory();
        Protocol mockProtocol = mock(Protocol.class);
        String query = "insert abc (c1,c2,c3) value (?,?,?) on duplicate key update c1 = values(c1)";
        ParameterizedBatchHandler rpbh = rpbhf.get(query, mockProtocol);
        assertTrue(rpbh instanceof RewriteParameterizedBatchHandler);
        for(int i = 0;i<3;i++) {
            ParameterizedQuery pq = new MySQLParameterizedQuery(query);
            pq.setParameter(0, new StringParameter("a"+i, false));
            pq.setParameter(1, new StringParameter("b"+i, false));
            pq.setParameter(2, new StringParameter("c"+i, false));
            rpbh.addToBatch(pq);
        }
        rpbh.executeBatch();
        String expectedQuery = "insert abc (c1,c2,c3) value('a0','b0','c0'),('a1','b1','c1'),('a2','b2','c2')on duplicate key update c1 = values(c1)";
        verify(mockProtocol).executeQuery(new MySQLQuery(expectedQuery));
    }

    /**
     * bug 501443
     * @throws QueryException
     * @throws IllegalParameterException
     */
    @Test
    public void testFullyQualifiedTable() throws QueryException, IllegalParameterException {
        RewriteParameterizedBatchHandlerFactory rpbhf = new RewriteParameterizedBatchHandlerFactory();
        Protocol mockProtocol = mock(Protocol.class);
        String query = "insert table1.abc (c1,c2,c3) value (?,?,?) on duplicate key update c1 = values(c1)";
        ParameterizedBatchHandler rpbh = rpbhf.get(query, mockProtocol);
        assertTrue(rpbh instanceof RewriteParameterizedBatchHandler);
        for(int i = 0;i<3;i++) {
            ParameterizedQuery pq = new MySQLParameterizedQuery(query);
            pq.setParameter(0, new StringParameter("a"+i,false));
            pq.setParameter(1, new StringParameter("b"+i,false));
            pq.setParameter(2, new StringParameter("c"+i, false));
            rpbh.addToBatch(pq);
        }
        rpbh.executeBatch();
        String expectedQuery = "insert table1.abc (c1,c2,c3) value('a0','b0','c0'),('a1','b1','c1'),('a2','b2','c2')on duplicate key update c1 = values(c1)";
        verify(mockProtocol).executeQuery(new MySQLQuery(expectedQuery));
    }
}
