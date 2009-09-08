package org.drizzle.jdbc.internal.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Jul 13, 2009 Time: 6:50:13 PM To change this template use File |
 * Settings | File Templates.
 */
public class RewriteParameterizedBatchHandlerFactory implements ParameterizedBatchHandlerFactory {

    public ParameterizedBatchHandler get(final String query, final Protocol protocol) {
        final Pattern p = Pattern.compile(
                "(?i)^\\s*+(INSERT (INTO)?\\s*\\w+\\s*(\\([^\\)]*\\))?\\s*VALUES?)\\s*(\\([^\\)]*\\))\\s*(ON DUPLICATE KEY UPDATE.+)?");
        final Matcher m = p.matcher(query);
        if (m.matches()) {
            return new RewriteParameterizedBatchHandler(protocol, m.group(1), m.group(4), m.group(5));
        } else {
            return new DefaultParameterizedBatchHandler(protocol);
        }
    }
}
