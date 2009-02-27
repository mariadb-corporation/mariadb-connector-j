package org.drizzle.jdbc.internal.query;

import java.io.Reader;
import java.io.OutputStream;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 9:35:10 PM
 */
public class ReaderParameter implements ParameterHolder {
    private final long length;
    private Reader reader;

    public ReaderParameter(Reader reader, long length) {
        this.reader=reader;
        this.length=length+2; // beginning and ending "
    }

    public void writeTo(OutputStream os) throws IOException {
        int ch;
        os.write('"');
        while((ch=reader.read())!=-1) {
            os.write(ch);
        }
        os.write('"');
    }

    public long length() {
        return length;
    }
}
