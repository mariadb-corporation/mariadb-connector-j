/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

import org.drizzle.jdbc.internal.common.Utils;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Since drizzle has no time datatype, jdbc time is stored in a packed integer
 *
 * @see Utils#packTime(long) 
 */
public class TimeParameter implements ParameterHolder{
    private final byte [] byteRepresentation;
    
    public TimeParameter(long timestamp) {
        int packedTime = Utils.packTime(timestamp);
        byteRepresentation = String.valueOf(packedTime).getBytes();
    }

    public void writeTo(OutputStream os) throws IOException {
        for(byte b:byteRepresentation)
            os.write(b);
    }

    public long length() {
        return byteRepresentation.length;
    }
}