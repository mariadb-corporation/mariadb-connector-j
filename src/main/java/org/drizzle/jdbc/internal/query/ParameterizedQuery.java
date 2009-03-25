package org.drizzle.jdbc.internal.query;

/**
 .
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:07:57 PM

 */
public interface ParameterizedQuery extends Query {

    int getParamCount();
    void clearParameters();
    public void setParameter(int position, ParameterHolder parameter) throws IllegalParameterException;
}
