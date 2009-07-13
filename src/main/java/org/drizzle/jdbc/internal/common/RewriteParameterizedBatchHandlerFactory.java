package org.drizzle.jdbc.internal.common;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jul 13, 2009
 * Time: 6:50:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class RewriteParameterizedBatchHandlerFactory implements ParameterizedBatchHandlerFactory {

    public ParameterizedBatchHandler get(String query, Protocol protocol) {
        Pattern p = Pattern.compile("(?i)^\\s*+(INSERT INTO.*VALUES)\\s*(\\(.*\\))");
        Matcher m = p.matcher(query);
        if(m.matches()) {
            return new RewriteParameterizedBatchHandler(protocol, m.group(1), m.group(2));
        } else {
            return new DefaultParameterizedBatchHandler(protocol);
        }
    }
}
