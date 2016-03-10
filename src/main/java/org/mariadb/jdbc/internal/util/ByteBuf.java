package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

public interface ByteBuf {
    
    int pos();
    
    int remaining();
    
    void recycle();

    void writeTo(OutputStream os) throws IOException;

    void incPos(int len);

    byte[] array();
    
    long address();

    void free();
    
}