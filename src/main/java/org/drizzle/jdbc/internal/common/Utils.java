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
    /**
     * returns true if the byte b needs to be escaped
     * @param b the byte to check
     * @return true if the byte needs escaping
     */

    public static boolean needsEscaping(byte b) {
        if((b <= (byte)127)) { // ascii signs are below 127
            switch(b) {
                case '"':
                case 0:
                case '\n':
                case '\r':
                case '\\':
                case '\'':
                    return true;
            }
        }
        return false;
    }
   /**
     * escapes the given string, new string length is at most twice the length of str 
     * @param str the string to escape
     * @return an escaped string
     */
    public static String sqlEscapeString(String str) {
        byte [] strBytes = str.getBytes();
        byte [] outBytes = new byte[strBytes.length*2]; //overkill but safe, streams need to be escaped on-the-fly
        int bytePointer=0;
        for(byte b : strBytes) {
            if(needsEscaping(b)) {
                outBytes[bytePointer++]='\\';
                outBytes[bytePointer++]=b;
            } else {
                outBytes[bytePointer++]=b;
            }
        }
        return new String(outBytes,0,bytePointer);
    }

    /**
     * returns count of characters c in str
     * @param str the string to count
     * @param c the character
     * @return the number of chars c in str
     */
    public static int countChars(String str, char c) {
        int count=0;
        for(byte b:str.getBytes()) {
            if(c == b) count++;
        }
        return count;
    }

    /**
     * encrypts a password
     *
     * protocol for authentication is like this:
     * 1. mysql server sends a random array of bytes (the seed)
     * 2. client makes a sha1 digest of the password
     * 3. client hashes the output of 2
     * 4. client digests the seed
     * 5. client updates the digest with the output from 3
     * 6. an xor of the output of 5 and 2 is sent to server
     * 7. server does the same thing and verifies that the scrambled passwords match
     * 
     *
     * @param password the password to encrypt
     * @param seed the seed to use
     * @return a scrambled password
     * @throws NoSuchAlgorithmException if SHA1 is not available on the platform we are using
     */
    public static byte[] encryptPassword(String password, byte [] seed) throws NoSuchAlgorithmException {
        if(password == null || password.equals("")) return new byte[0];
        
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        byte [] stage1 = messageDigest.digest(password.getBytes());
        messageDigest.reset();

        byte [] stage2 = messageDigest.digest(stage1);
        messageDigest.reset();

        messageDigest.update(seed);
        messageDigest.update(stage2);
        
        byte[] digest = messageDigest.digest();
        byte[] returnBytes = new byte[digest.length];
        for(int i = 0 ; i<digest.length; i++) {
            returnBytes[i] = (byte) (stage1[i]^digest[i]);            
        }
        return returnBytes;
    }

    /**
     * packs the time portion of a millisecond time stamp into an int
     *
     * format:
     * <pre>
     * 5 bytes| 6 bytes | 6 bytes  | the rest    |
     * 0 .. 4 | 5 .. 11 | 12 .. 17 | 18 .. 32    |
     *  hours | minutes | seconds  | millis      |
     * </pre>
     * @param milliseconds the milliseconds to pack
     * @return a packed integer containing the time
     */

    public static int packTime(long milliseconds) {
        int millis = (int) (milliseconds%1000);
        int seconds = (int) ((milliseconds/1000)%60);
        int minutes= (int) ((milliseconds/(1000*60))%60);
        int hours = (int) ((milliseconds/(1000*60*60))%24);
        int storeVal = hours<<(32-5); // five bytes
        storeVal = storeVal | (minutes << (32-11)); // 6 bytes
        storeVal = storeVal | (seconds << (32-17)); // 6 bytes
        storeVal = storeVal | millis;  // the rest
        return storeVal;
    }

    /**
     * unpacks an integer packed by packTime
     * @see Utils#packTime(long)
     * @param packedTime the packed time
     * @return a millisecond time
     */
    public static long unpackTime(int packedTime) {
        long hours = (packedTime >> 32-5)&0x1f;
        int last6bits = 0x3f;
        long minutes = packedTime >> ((32 - 11)) & last6bits;
        long seconds = packedTime >> ((32 - 17)) & last6bits;
        long millis = packedTime & 0xfff;
        long returnValue = (hours * 60*60*1000);
        returnValue += (minutes*60*1000);
        returnValue += seconds * 1000;
        returnValue += millis;
        return returnValue;
    }
}
