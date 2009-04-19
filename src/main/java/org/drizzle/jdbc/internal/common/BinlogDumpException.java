package org.drizzle.jdbc.internal.common;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Apr 19, 2009
 * Time: 9:24:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class BinlogDumpException extends Throwable {
    public BinlogDumpException(String s, IOException e) {
        super(s,e);
    }
}
