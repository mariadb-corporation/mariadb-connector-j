package org.mariadb.jdbc.internal.util;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class UnsafeString {
    
    private static final long VALUE_OFFSET;
    
    private static final Unsafe UNSAFE;
    
    static {
        UNSAFE = UnsafeUtil.unsafe();
        try {
            VALUE_OFFSET = UNSAFE.objectFieldOffset(String.class.getDeclaredField("value"));
        } catch (NoSuchFieldException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static char[] getChars(String str) {
        return (char[]) UNSAFE.getObject(str, VALUE_OFFSET);
    }
    
}