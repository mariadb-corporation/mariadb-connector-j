/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import static org.drizzle.jdbc.internal.common.Utils.needsEscaping;
import org.drizzle.jdbc.internal.common.query.parameters.ParameterHolder;
import org.drizzle.jdbc.internal.common.Utils;

import java.io.*;
import java.util.Arrays;

/**
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:53:14 PM
 */
public class SerializableParameter implements ParameterHolder {
    private final byte[] rawBytes;
    private final int length;

    public SerializableParameter(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        rawBytes = escapeBytes(baos.toByteArray());
        length = rawBytes.length;
    }

    public void writeTo(OutputStream os) throws IOException {
        os.write(rawBytes,0,length);
    }

    public long length() {
        return length;
    }

    private static byte [] escapeBytes(byte[] input) {
        byte [] buffer = new byte[input.length*2+2];
        int i=0;
        buffer[i++]='\"';
        for(byte b : input) {
            if(Utils.needsEscaping(b))
                buffer[i++] = '\\';
            buffer[i++] = b;
        }
        buffer[i++]='\"';
        return Arrays.copyOf(buffer,i);
    }
}