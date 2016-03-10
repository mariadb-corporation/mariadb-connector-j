package org.mariadb.jdbc.internal.util;

import java.io.IOException;
import java.io.OutputStream;

import sun.misc.Unsafe;

@SuppressWarnings("all")
final class ByteBufUnsafe implements ByteBuf {
    
    private static final Unsafe UNSAFE = UnsafeUtil.unsafe();
    
    private final long adr;
    private int pos;
    
    public static final int DEFAULT_PAGE = 2048;
    
    private final byte[] values = new byte[DEFAULT_PAGE];
    
    public ByteBufUnsafe() {
        adr = UNSAFE.allocateMemory(DEFAULT_PAGE);
        pos = 0;
    }
    
    @Override
    public int pos() {
        return pos;
    }
    
    @Override
    public int remaining() {
        return DEFAULT_PAGE - pos;
    }
    
    @Override
    public void recycle() {
        pos = 0;
    }
    
    @Override
    public void writeTo(OutputStream os) throws IOException {
        byte[] values = this.values;
        UNSAFE.copyMemory(null, adr, values, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, pos);
        os.write(values, 0, pos);
    }
    
    @Override
    public void incPos(int len) {
        pos += len;
    }
    
    @Override
    public byte[] array() {
        byte[] values = this.values;
        UNSAFE.copyMemory(null, adr, values, UnsafeUtil.BYTE_ARRAY_BASE_OFFSET, pos);
        return values;
    }
    
    @Override
    public long address() {
        return adr;
    }
    
    @Override
    public void free() {
        UNSAFE.freeMemory(adr);
    }
    
}
