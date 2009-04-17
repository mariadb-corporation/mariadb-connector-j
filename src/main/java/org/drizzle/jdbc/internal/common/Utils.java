package org.drizzle.jdbc.internal.common;

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

}
