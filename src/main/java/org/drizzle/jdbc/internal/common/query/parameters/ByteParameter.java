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
import static org.drizzle.jdbc.internal.common.Utils.needsEscaping;

import java.io.OutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 .
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 9:56:17 PM

 */
public class ByteParameter implements ParameterHolder {
    private final byte[] buffer;
    private final int length;

    public ByteParameter(byte[] x) {
        buffer = new byte[x.length*2+2];
        int pos=0;
        buffer[pos++] = '"';
        for(byte b:x) {
            if(needsEscaping(b))
                buffer[pos++]='\\';
            buffer[pos++]=b; 
        }
        buffer[pos++] = '"';
        this.length = pos; 
    }


    public void writeTo(OutputStream os) throws IOException {
        os.write(buffer,0,length);
    }

    public long length() {
        return length;
    }
}
