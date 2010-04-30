package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * Very basic info about the parameterized query, only reliable method is getParameterCount();
 */
public class DrizzleParameterMetaData implements ParameterMetaData {
    private final ParameterizedQuery query;

    public DrizzleParameterMetaData(ParameterizedQuery dQuery) {
        this.query = dQuery;
    }

    public int getParameterCount() throws SQLException {
        return query.getParamCount();
    }

    public int isNullable(int i) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    public boolean isSigned(int i) throws SQLException {
        return true;
    }

    //TODO: fix
    public int getPrecision(int i) throws SQLException {
        return 1;
    }

    //TODO: fix
    public int getScale(int i) throws SQLException {
        return 0;
    }

    //TODO: fix
    public int getParameterType(int i) throws SQLException {
        return java.sql.Types.VARCHAR;
    }

    //TODO: fix
    public String getParameterTypeName(int i) throws SQLException {
        return "String";
    }

    //TODO: fix
    public String getParameterClassName(int i) throws SQLException {
        return "String.class";
    }

    public int getParameterMode(int i) throws SQLException {
        return parameterModeInOut;
    }

    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
