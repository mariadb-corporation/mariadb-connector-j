package org.drizzle.jdbc.internal.query;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 9:35:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class NullParameter implements ParameterHolder {
    private final byte[] byteRepresentation;
    private int bytePointer=0;
    public NullParameter() {
        this.byteRepresentation = "NULL".getBytes();
    }
    public byte read() {
        return byteRepresentation[bytePointer++];
    }

    public long length() {
        return byteRepresentation.length;
    }
}
