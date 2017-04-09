package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.util.dao.PrepareResult;

import java.sql.SQLException;

public class AsyncMultiReadResult {
    private PrepareResult prepareResult;
    private SQLException exception;

    public AsyncMultiReadResult(PrepareResult prepareResult) {
        this.prepareResult = prepareResult;
        exception = null;
    }

    public PrepareResult getPrepareResult() {
        return prepareResult;
    }

    public void setPrepareResult(PrepareResult prepareResult) {
        this.prepareResult = prepareResult;
    }

    public SQLException getException() {
        return exception;
    }

    public void setException(SQLException exception) {
        this.exception = exception;
    }
}
