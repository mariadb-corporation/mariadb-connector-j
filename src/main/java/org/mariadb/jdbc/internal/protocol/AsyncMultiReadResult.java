package org.mariadb.jdbc.internal.protocol;

import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.QueryException;

public class AsyncMultiReadResult {
    private PrepareResult prepareResult;
    private QueryException exception;

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

    public QueryException getException() {
        return exception;
    }

    public void setException(QueryException exception) {
        this.exception = exception;
    }
}
