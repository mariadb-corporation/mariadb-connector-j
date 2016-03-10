package org.mariadb.jdbc.internal.util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public final class UnsafeUtil {
    
    private static final Unsafe UNSAFE;
    
    public static final long BYTE_ARRAY_BASE_OFFSET;
    
    public static final long CHAR_ARRAY_BASE_OFFSET;
    
    static {
        
        final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
            @Override
            public Unsafe run() throws NoSuchFieldException, IllegalAccessException {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(null);
            }
        };
        
        try {
            UNSAFE = AccessController.doPrivileged(action);
        } catch (PrivilegedActionException cause) {
            throw new RuntimeException("Unable to load unsafe", cause);
        }
        
        BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        CHAR_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(char[].class);
        
    }
    
    private UnsafeUtil() {
    }
    
    public static Unsafe unsafe() {
        return UNSAFE;
    }
}
