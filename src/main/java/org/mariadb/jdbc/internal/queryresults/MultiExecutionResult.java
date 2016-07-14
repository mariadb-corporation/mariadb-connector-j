package org.mariadb.jdbc.internal.queryresults;

public interface MultiExecutionResult extends ExecutionResult {

    void updateResultsForRewrite(int waitedSize, boolean hasException);

    void updateResultsMultiple(int waitedSize, boolean hasException);

    int[] getAffectedRows();
}

