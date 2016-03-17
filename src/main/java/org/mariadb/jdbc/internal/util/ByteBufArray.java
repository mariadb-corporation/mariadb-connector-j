package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

final class ByteBufArray implements ByteBuf {
    
    public static final int DEFAULT_PAGE = 1024;
    
    private final byte[] array;
    
    private final int len;
    
    protected int pos;
    
    private final int off; 
    
    public ByteBufArray() {
        array = new byte[DEFAULT_PAGE];
        pos = 0;
        off = 0;
        len = DEFAULT_PAGE;
    }
    
    public ByteBufArray(byte[] src, int off, int len) {
        array = src;
        pos = len;
        this.off = off;
        this.len = len;
    }
    
    public int remaining() {
        return DEFAULT_PAGE - pos;
    }
    
    @Override
    public void recycle() {
        pos = 0;
    }
    
    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(array, off, pos);
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public byte[] array() {
        return array;
    }

    @Override
    public long address() {
        return 0;
    }

    @Override
    public void free() {
    }

    @Override
    public int pos(int inc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pos(long address) {
        throw new UnsupportedOperationException();
    }

//    @Override
//    public int writePartTo(OutputStream outputStream, int length) throws IOException {
//        outputStream.write(array, off + pos, Math.min(length, len));
//        pos += Math.min(length, len);
//        return pos;
//    }
    
}
