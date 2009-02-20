package org.drizzle.jdbc.internal.query;

import static org.drizzle.jdbc.internal.Utils.sqlEscapeString;

import java.io.OutputStream;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Feb 18, 2009
 * Time: 10:17:14 PM
 */
public class StringParameter implements ParameterHolder {
    private final byte [] byteRepresentation;

    public StringParameter(String parameter) {
        String tempParam = "\""+sqlEscapeString(parameter)+"\"";
        this.byteRepresentation=tempParam.getBytes();
    }

    public void writeTo(OutputStream os) throws IOException {
        for(byte b:byteRepresentation)
            os.write(b);
    }

    public long length() {
        return byteRepresentation.length;
    }
}
