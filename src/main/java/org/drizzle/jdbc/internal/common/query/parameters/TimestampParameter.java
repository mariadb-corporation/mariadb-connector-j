/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Represents a time stamp
 * <p/>
 * User: marcuse Date: Feb 19, 2009 Time: 8:50:52 PM
 */
public class TimestampParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    /**
     * Represents a timestamp, constructed with time in millis since epoch
     *
     * @param timestamp the time in millis since epoch
     */
    public TimestampParameter(final long timestamp) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        byteRepresentation = String.valueOf("\"" + sdf.format(new Date(timestamp)) + "\"").getBytes();
    }

    public TimestampParameter(final long timestamp, final Calendar cal) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setCalendar(cal);
        byteRepresentation = String.valueOf("\"" + sdf.format(new Date(timestamp)) + "\"").getBytes();

    }

    public void writeTo(final OutputStream os) throws IOException {
        os.write(byteRepresentation);
    }

    public long length() {
        return byteRepresentation.length;
    }
}