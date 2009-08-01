package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;
import org.drizzle.jdbc.internal.common.query.DrizzleQuery;
import org.drizzle.jdbc.internal.common.query.parameters.ParameterHolder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.logging.Logger;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Statement;

/**
 * Rewrites queries on the form (INSERT INTO xyz (a,b,c) VALUES (?,?,?))* to
 * INSERT INTO xyz (a,b,c) VALUES ((?,?,?),)*
 */
public class RewriteParameterizedBatchHandler implements ParameterizedBatchHandler {
    private final static Logger log = Logger.getLogger(RewriteParameterizedBatchHandler.class.getName());
    private final int MAX_QUERY_LENGTH = 1000000;
    private final String baseQuery;
    private final String queryValuePart;
    private final Protocol protocol;
    private final List<String> queriesToSend = new LinkedList<String>();
    private StringBuilder queryBuilder = new StringBuilder();
    private boolean firstWritten = false;
    private int queryCount = 0;
    private final String onDupKeyPart;

    /**
     * Constructs a new handler
     * @param protocol the protocol to use to send the query.
     * @param baseQuery the base of the query, i.e. everything including .*VALUES
     * @param queryValuePart the value part (?,?..)
     * @param onDupKeyPart the duplicate key part of the query
     */
    public RewriteParameterizedBatchHandler(Protocol protocol, String baseQuery, String queryValuePart, String onDupKeyPart) {
        this.baseQuery = baseQuery;
        this.queryValuePart = queryValuePart;
        this.onDupKeyPart = (onDupKeyPart == null?"":onDupKeyPart);
        queryBuilder.append(baseQuery);

        this.protocol = protocol;
    }

    public void addToBatch(ParameterizedQuery query) {
        Map<Integer, ParameterHolder> parameters = query.getParameters();
        StringBuilder replacedValuePart = new StringBuilder();
        int questionMarkPosition = 0;

        for(int i = 0;i<queryValuePart.length();i++) {
            char ch = queryValuePart.charAt(i);
            if(ch == '?') {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ParameterHolder parameterHolder = parameters.get(questionMarkPosition++);
                try {
                    parameterHolder.writeTo(baos); // writeTo escapes and adds quotes etc
                } catch (IOException e) {
                    throw new RuntimeException("Could not write to byte array: "+e.getMessage(),e);
                }
                replacedValuePart.append(baos.toString());
            } else {
                replacedValuePart.append(ch);
            }
        }
        String replaced = replacedValuePart.toString();

        if(queryBuilder.length() + replaced.length() + onDupKeyPart.length() > MAX_QUERY_LENGTH) {
            queryBuilder.append(onDupKeyPart);
            queriesToSend.add(queryBuilder.toString());
            queryBuilder = new StringBuilder();
            queryBuilder.append(baseQuery);

            firstWritten = false;
        }

        if(firstWritten) {
            queryBuilder.append(",");
        }

        queryBuilder.append(replaced);
        firstWritten = true;
        queryCount++;
    }

    public int[] executeBatch() throws QueryException {
        queryBuilder.append(onDupKeyPart);
        String lastQuery = queryBuilder.toString();

        if(firstWritten) {
            queriesToSend.add(lastQuery);
        }

        for(String query : queriesToSend) {
            protocol.executeQuery(new DrizzleQuery(query));
        }

        log.finest("Rewrote "+queryCount+" queries to "+queriesToSend.size()+" queries");
        int [] returnArray = new int[queryCount];
        Arrays.fill(returnArray, Statement.SUCCESS_NO_INFO);
        return returnArray;
    }
}
