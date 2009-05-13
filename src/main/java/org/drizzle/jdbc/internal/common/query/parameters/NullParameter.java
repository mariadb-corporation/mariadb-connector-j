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

/**
 .
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 9:35:15 PM

 */
public class NullParameter implements ParameterHolder {
    private final byte[] byteRepresentation;
    private int bytePointer=0;
    public NullParameter() {
        this.byteRepresentation = "NULL".getBytes();
    }
    
    public void writeTo(OutputStream os) throws IOException {
        for(byte b:byteRepresentation)
            os.write(b);
    }

    public long length() {
        return byteRepresentation.length;
    }
}
