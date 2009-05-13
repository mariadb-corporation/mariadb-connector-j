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

import java.io.Reader;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 .
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 9:53:04 PM

 */
public class BufferedReaderParameter implements ParameterHolder {
    private final int length;
    private final byte[] byteRepresentation;

    public BufferedReaderParameter(Reader reader) throws IOException {
        byte b;
        byte [] tempByteRepresentation=new byte[1000];
        int pos = 0;
        tempByteRepresentation[pos++]=(byte)'"';
        while((b= (byte) reader.read())!=-1) {
            if(pos>tempByteRepresentation.length-2) { //need two places in worst case
                tempByteRepresentation= Arrays.copyOf(tempByteRepresentation,tempByteRepresentation.length*2);
            }
            if(needsEscaping(b))
                tempByteRepresentation[pos++]='\\';
            tempByteRepresentation[pos++]=b;
        }
        tempByteRepresentation[pos++]=(byte)'"';
        length=pos;
        byteRepresentation=tempByteRepresentation;
    }

    public void writeTo(OutputStream os) throws IOException {
        for(int i=0;i<length;i++) {
            os.write(byteRepresentation[i]);
        }
    }

    public long length() {
        return length;
    }
}
