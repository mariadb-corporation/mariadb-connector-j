package org.drizzle.jdbc.internal.query;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:07:57 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ParameterizedQuery extends Query {

    int getParamCount();
    void clearParameters();
    public void setParameter(int position, ParameterHolder parameter) throws IllegalParameterException;
}
