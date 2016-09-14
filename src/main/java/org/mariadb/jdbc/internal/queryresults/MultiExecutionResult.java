package org.mariadb.jdbc.internal.queryresults;

public interface MultiExecutionResult extends ExecutionResult {

    int[] updateResultsForRewrite(int waitedSize, boolean hasException);

    int[] updateResultsMultiple(int waitedSize, boolean hasException);

    int[] getAffectedRows();

    int getFirstAffectedRows();
}

