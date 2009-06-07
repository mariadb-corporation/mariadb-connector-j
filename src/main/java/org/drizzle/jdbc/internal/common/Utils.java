/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:40:51 PM
 */
public class Utils {
    private static final int START_BIT_MILLISECONDS = 17;
    private static final int START_BIT_SECONDS = 11;
    private static final int START_BIT_MINUTES = 5;
    /*
     * These masks allow us to "cut up" the packed integer into
     * its components.
     */
    private static final int MASK_HOURS = 0x0000001F;
    private static final int MASK_MINUTES = 0x000007E0;
    private static final int MASK_SECONDS = 0x0001F800;
    private static final int MASK_MILLISECONDS = 0xFFFE0000;

    /**
     * returns true if the byte b needs to be escaped
     *
     * @param b the byte to check
     * @return true if the byte needs escaping
     */

    public static boolean needsEscaping(byte b) {
        if ((b & 0x80) == 0) {
            switch (b) {
                case '"':
                case 0:
                case '\n':
                case '\r':
                case '\\':
                case '\'':
                case '\032':
                    return true;
            }
        }
        return false;
    }

    /*public static byte [] escape(byte [] input) {
        byte [] buffer = new byte[input.length*2];
        for(byte b:buffer) {
            if(b <= (byte)127) {
                switch(b) {
                    case 0:
                    case '"':
                    case '\n':
                    case '\r':
                    case '\\':
                    case '\'':
                    case '\032':
                    
                }
            }
        }

    } */
    /**
     * escapes the given string, new string length is at most twice the length of str
     *
     * @param str the string to escape
     * @return an escaped string
     */
    public static String sqlEscapeString(String str) {
        byte[] strBytes = str.getBytes();
        byte[] outBytes = new byte[strBytes.length * 2]; //overkill but safe, streams need to be escaped on-the-fly
        int bytePointer = 0;
        for (byte b : strBytes) {
            if (needsEscaping(b)) {
                outBytes[bytePointer++] = '\\';
                outBytes[bytePointer++] = b;
            } else {
                outBytes[bytePointer++] = b;
            }
        }
        return new String(outBytes, 0, bytePointer);
    }

    /**
     * returns count of characters c in str
     *
     * @param str the string to count
     * @param c   the character
     * @return the number of chars c in str
     */
    public static int countChars(String str, char c) {
        int count = 0;
        for (byte b : str.getBytes()) {
            if (c == b) count++;
        }
        return count;
    }

    /**
     * encrypts a password
     * <p/>
     * protocol for authentication is like this:
     * 1. mysql server sends a random array of bytes (the seed)
     * 2. client makes a sha1 digest of the password
     * 3. client hashes the output of 2
     * 4. client digests the seed
     * 5. client updates the digest with the output from 3
     * 6. an xor of the output of 5 and 2 is sent to server
     * 7. server does the same thing and verifies that the scrambled passwords match
     *
     * @param password the password to encrypt
     * @param seed     the seed to use
     * @return a scrambled password
     * @throws NoSuchAlgorithmException if SHA1 is not available on the platform we are using
     */
    public static byte[] encryptPassword(String password, byte[] seed) throws NoSuchAlgorithmException {
        if (password == null || password.equals("")) return new byte[0];

        MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        byte[] stage1 = messageDigest.digest(password.getBytes());
        messageDigest.reset();

        byte[] stage2 = messageDigest.digest(stage1);
        messageDigest.reset();

        messageDigest.update(seed);
        messageDigest.update(stage2);

        byte[] digest = messageDigest.digest();
        byte[] returnBytes = new byte[digest.length];
        for (int i = 0; i < digest.length; i++) {
            returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
        }
        return returnBytes;
    }

    /**
     * packs the time portion of a millisecond time stamp into an int
     * <p/>
     * format:
     * <pre>
     * The part of the day, stored in a 4 byte integer as follows:
     * <p/>
     * | 31 | 30 | 29 | 28 | 27 | 26 | 25 | 24 |
     * | mS | mS | mS | mS | mS | mS | mS | mS |
     * | 23 | 22 | 21 | 20 | 19 | 18 | 17 | 16 |
     * | mS | mS | mS | mS | mS | mS | mS | SS |
     * | 15 | 14 | 13 | 12 | 11 | 10 | 09 | 08 |
     * | SS | SS | SS | SS | SS | MM | MM | MM |
     * | 07 | 06 | 05 | 04 | 03 | 02 | 01 | 00 |
     * | MM | MM | MM | HH | HH | HH | HH | HH |
     * </pre>
     *
     * @param milliseconds the milliseconds to pack
     * @return a packed integer containing the time
     */


    public static int packTime(long milliseconds) {
        int millis = (int) (milliseconds % 1000);
        int seconds = (int) ((milliseconds / 1000) % 60);
        int minutes = (int) ((milliseconds / (1000 * 60)) % 60);
        int hours = (int) ((milliseconds / (1000 * 60 * 60)) % 24);
        /* OK, now we pack the pieces into a 4-byte integer */
        return ((int) millis * (1 << START_BIT_MILLISECONDS))
                + ((int) seconds * (1 << START_BIT_SECONDS))
                + ((int) minutes * (1 << START_BIT_MINUTES))
                + hours;
    }

    /**
     * unpacks an integer packed by packTime
     *
     * @param packedTime the packed time
     * @return a millisecond time
     * @see Utils#packTime(long)
     */
    public static long unpackTime(int packedTime) {
        int hours = (packedTime & MASK_HOURS);
        int minutes = (packedTime & MASK_MINUTES) >> (START_BIT_MINUTES);
        int seconds = (packedTime & MASK_SECONDS) >> (START_BIT_SECONDS);
        int millis = (packedTime & MASK_MILLISECONDS) >> (START_BIT_MILLISECONDS);
        long returnValue = (long) hours * 60 * 60 * 1000;
        returnValue += (long) minutes * 60 * 1000;
        returnValue += (long) seconds * 1000;
        returnValue += (long) millis;
        return returnValue;
    }
}