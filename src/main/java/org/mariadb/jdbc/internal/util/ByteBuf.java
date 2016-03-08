package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

public abstract class ByteBuf {
    
    public abstract int pos();
    
    public abstract int remaining();
    
    public abstract void recycle();

    public abstract void writeTo(OutputStream os) throws IOException;

    public abstract void incPos(int len);

    public abstract byte[] array();
    
}
