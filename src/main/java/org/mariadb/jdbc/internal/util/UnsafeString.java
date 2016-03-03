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

	public final static String buildUnsafe(char[] chars, int offset, int length) {
		String mutable = new String();// an empty string to hack
		UNSAFE.putObject(mutable, VALUE_OFFSET, chars);
		return mutable;
	}

	public final static char[] getChars(String s) {
		return (char[]) UNSAFE.getObject(s, VALUE_OFFSET);
	}
	
}