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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 .
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:56:34 PM

 */
public class BufferedStreamParameter implements ParameterHolder {
    private byte [] byteRepresentation;
    private int length;

    public BufferedStreamParameter(InputStream is) throws IOException {
        int b;
        byte [] tempByteRepresentation=new byte[1000];
        int pos = 0;
        tempByteRepresentation[pos++]='"';
        while((b=  is.read())!=-1) {
            if(pos>tempByteRepresentation.length-2) { //need two places in worst case
                tempByteRepresentation=Arrays.copyOf(tempByteRepresentation,tempByteRepresentation.length*2);
            }
            if(needsEscaping((byte)(b&0xff)))
                tempByteRepresentation[pos++]='\\';
            tempByteRepresentation[pos++]=(byte)(b&0xff);
        }
        tempByteRepresentation[pos++]='"';
        length=pos;
        byteRepresentation=tempByteRepresentation;

    }

    public void writeTo(OutputStream os) throws IOException {
        os.write(byteRepresentation,0,length);
    }

    public long length() {
        return length;
    }
}