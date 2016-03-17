package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

public interface ByteBuf {
    
    int pos();
    
    int pos(int inc);
    
    void pos(long address);
    
    int remaining();
    
    void recycle();

    void writeTo(OutputStream os) throws IOException;

    byte[] array();
    
    long address();

    void free();

    
    
}