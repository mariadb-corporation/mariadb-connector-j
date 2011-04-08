/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Jay Pipes
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.drizzle.jdbc.internal.common;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * User: marcuse Date: Feb 19, 2009 Time: 8:40:51 PM
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

    private static boolean java5Determined = false;
    private static boolean isJava5 = false;

    /**
     * returns true if the byte b needs to be escaped
     *
     * @param b the byte to check
     * @return true if the byte needs escaping
     */

    public static boolean needsEscaping(final byte b) {
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
    public static String sqlEscapeString(final String str) {
        byte[] strBytes = new byte[0];
        try {
            strBytes = str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
        byte[] outBytes = new byte[strBytes.length * 2]; //overkill but safe, streams need to be escaped on-the-fly
        int bytePointer = 0;
        boolean neededEscaping = false;
        for (final byte b : strBytes) {
            if (needsEscaping(b)) {
                neededEscaping = true;
                outBytes[bytePointer++] = '\\';
                outBytes[bytePointer++] = b;
            } else {
                outBytes[bytePointer++] = b;
            }
        }
        if(neededEscaping)
            try {
                return new String(outBytes, 0, bytePointer, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("UTF-8 not supported", e);
            }
        else
            return str;
    }

    /**
     * returns count of characters c in str.
     * <p/>
     * Does not count chars enclosed in single or double quotes
     *
     * @param str the string to count
     * @param c   the character
     * @return the number of chars c in str
     */
    public static int countChars(final String str, final char c) {
        int count = 0;
        boolean isWithinDoubleQuotes = false;
        boolean isWithinQuotes = false;

        for (final byte b : str.getBytes()) {
            if (b == '"' && !isWithinQuotes && !isWithinDoubleQuotes) {
                isWithinDoubleQuotes = true;
            } else if (b == '"' && !isWithinQuotes) {
                isWithinDoubleQuotes = false;
            }

            if (b == '\'' && !isWithinQuotes && !isWithinDoubleQuotes) {
                isWithinQuotes = true;
            } else if (b == '\'' && !isWithinDoubleQuotes) {
                isWithinQuotes = false;
            }

            if (!isWithinDoubleQuotes && !isWithinQuotes) {
                if (c == b) {
                    count++;
                }
            }
        }
        return count;
    }


    public static List<String> createQueryParts(String query) {
        boolean isWithinDoubleQuotes = false;
        boolean isWithinQuotes = false;
        int queryPos = 0;
        int lastQueryPos = 0;
        List<String> queryParts = new LinkedList<String>();

        byte [] queryBytes;
        try {
            queryBytes = query.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
        
        for (final byte b : queryBytes) {

            if (b == '"' && !isWithinQuotes && !isWithinDoubleQuotes) {
                isWithinDoubleQuotes = true;
            } else if (b == '"' && !isWithinQuotes) {
                isWithinDoubleQuotes = false;
            }

            if (b == '\'' && !isWithinQuotes && !isWithinDoubleQuotes) {
                isWithinQuotes = true;
            } else if (b == '\'' && !isWithinDoubleQuotes) {
                isWithinQuotes = false;
            }

            if (!isWithinDoubleQuotes && !isWithinQuotes) {
                if (b == '?') {
                    queryParts.add(query.substring(lastQueryPos, queryPos));
                    lastQueryPos = queryPos + 1;
                }
            }
            queryPos++;
        }
        queryParts.add(query.substring(lastQueryPos, queryPos));
        return queryParts;
    }


    private enum ParsingState {
        WITHIN_COMMENT, WITHIN_QUOTES, WITHIN_DOUBLE_QUOTES, NORMAL
    }

    public static String stripQuery(final String query) {
        final StringBuilder sb = new StringBuilder();
        ParsingState parsingState = ParsingState.NORMAL;
        ParsingState nextParsingState = ParsingState.NORMAL;
        //final byte[] queryBytes = query.getBytes();

        for (int i = 0; i < query.length(); i++) {
            final int b = query.codePointAt(i);
            int nextCodePoint = 0;

            if (i < query.length() - 1) {
                nextCodePoint = query.codePointAt(i + 1);
            }

            switch (parsingState) {
                case WITHIN_DOUBLE_QUOTES:
                    if (b == '"') {
                        nextParsingState = ParsingState.NORMAL;
                    }
                    break;
                case WITHIN_QUOTES:
                    if (b == '\'') {
                        nextParsingState = ParsingState.NORMAL;
                    }
                    break;
                case NORMAL:
                    if (b == '\'') {
                        nextParsingState = ParsingState.WITHIN_QUOTES;
                    } else if (b == '"') {
                        nextParsingState = ParsingState.WITHIN_DOUBLE_QUOTES;
                    } else if (b == '/' && nextCodePoint == '*') {
                        nextParsingState = ParsingState.WITHIN_COMMENT;
                        parsingState = ParsingState.WITHIN_COMMENT;
                    } else if (b == '#') {
                        return sb.toString();
                    }
                    break;
                case WITHIN_COMMENT:
                    if (b == '*' && nextCodePoint == '/') {
                        nextParsingState = ParsingState.NORMAL;
                        i++;
                    }
                    break;
            }

            if (parsingState != ParsingState.WITHIN_COMMENT) {
                sb.append((char) b);
            }
            parsingState = nextParsingState;
        }
        return sb.toString();
    }

    /**
     * encrypts a password
     * <p/>
     * protocol for authentication is like this: 1. mysql server sends a random array of bytes (the seed) 2. client
     * makes a sha1 digest of the password 3. client hashes the output of 2 4. client digests the seed 5. client updates
     * the digest with the output from 3 6. an xor of the output of 5 and 2 is sent to server 7. server does the same
     * thing and verifies that the scrambled passwords match
     *
     * @param password the password to encrypt
     * @param seed     the seed to use
     * @return a scrambled password
     * @throws NoSuchAlgorithmException if SHA1 is not available on the platform we are using
     */
    public static byte[] encryptPassword(final String password, final byte[] seed) throws NoSuchAlgorithmException {
        if (password == null || password.equals("")) {
            return new byte[0];
        }

        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        final byte[] stage1 = messageDigest.digest(password.getBytes());
        messageDigest.reset();

        final byte[] stage2 = messageDigest.digest(stage1);
        messageDigest.reset();

        messageDigest.update(seed);
        messageDigest.update(stage2);

        final byte[] digest = messageDigest.digest();
        final byte[] returnBytes = new byte[digest.length];
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


    public static int packTime(final long milliseconds) {
        final int millis = (int) (milliseconds % 1000);
        final int seconds = (int) ((milliseconds / 1000) % 60);
        final int minutes = (int) ((milliseconds / (1000 * 60)) % 60);
        final int hours = (int) ((milliseconds / (1000 * 60 * 60)) % 24);
        /* OK, now we pack the pieces into a 4-byte integer */
        return (millis * (1 << START_BIT_MILLISECONDS))
                + (seconds * (1 << START_BIT_SECONDS))
                + (minutes * (1 << START_BIT_MINUTES))
                + hours;
    }

    /**
     * unpacks an integer packed by packTime
     *
     * @param packedTime the packed time
     * @return a millisecond time
     * @see Utils#packTime(long)
     */
    public static long unpackTime(final int packedTime) {
        final int hours = (packedTime & MASK_HOURS);
        final int minutes = (packedTime & MASK_MINUTES) >> (START_BIT_MINUTES);
        final int seconds = (packedTime & MASK_SECONDS) >> (START_BIT_SECONDS);
        final int millis = (packedTime & MASK_MILLISECONDS) >> (START_BIT_MILLISECONDS);
        long returnValue = (long) hours * 60 * 60 * 1000;
        returnValue += (long) minutes * 60 * 1000;
        returnValue += (long) seconds * 1000;
        returnValue += (long) millis;
        return returnValue;
    }

    /**
     * Copies the original byte array content to a new byte array. The resulting byte array is
     * always "length" size. If length is smaller than the original byte array, the resulting
     * byte array is truncated. If length is bigger than the original byte array, the resulting
     * byte array is filled with zero bytes.
     *
     * @param orig the original byte array
     * @param length how big the resulting byte array will be
     * @return the copied byte array
     */
    public static byte[] copyWithLength(byte[] orig, int length) {
        // No need to initialize with zero bytes, because the bytes are already initialized with that
        byte[] result = new byte[length];
        int howMuchToCopy = length < orig.length ? length : orig.length;
        System.arraycopy(orig, 0, result, 0, howMuchToCopy);
        return result;
    }

    /**
     * Copies from original byte array to a new byte array. The resulting byte array is
     * always "to-from" size.
     *
     * @param orig the original byte array
     * @param from index of first byte in original byte array which will be copied
     * @param to index of last byte in original byte array which will be copied. This can be
     *           outside of the original byte array
     * @return resulting array
     */
    public static byte[] copyRange(byte[] orig, int from, int to) {
        int length = to - from;
        byte[] result = new byte[length];
        int howMuchToCopy = orig.length - from < length ? orig.length - from : length;
        System.arraycopy(orig, from, result, 0, howMuchToCopy);
        return result;
    }

    /**
     * Returns if it is a Java version up to Java 5.
     *
     * @return true if the VM is <= Java 5
     */
    public static boolean isJava5() {
        if (!java5Determined) {
            try {
                java.util.Arrays.copyOf(new byte[0], 0);
                isJava5 = false;
            } catch (java.lang.NoSuchMethodError e) {
                 isJava5 = true;
            }
            java5Determined = true;
        }
        return isJava5;
    }
}