/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import org.drizzle.jdbc.internal.common.query.parameters.ParameterHolder;

import java.io.OutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

/**
 * Represents a time stamp
 *
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:50:52 PM

 */
public class DateParameter implements ParameterHolder {
    private final byte [] byteRepresentation;

    /**
     * Represents a timestamp, constructed with time in millis since epoch
     * @param timestamp the time in millis since epoch
     */
    public DateParameter(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        byteRepresentation = String.valueOf("\""+sdf.format(new Date(timestamp))+"\"").getBytes();
    }

    public DateParameter(long timestamp, Calendar cal) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setCalendar(cal);
        byteRepresentation = String.valueOf("\""+sdf.format(new Date(timestamp))+"\"").getBytes();

    }

    public void writeTo(OutputStream os) throws IOException {
        for(byte b:byteRepresentation)
            os.write(b);
    }

    public long length() {
        return byteRepresentation.length;
    }
}