package org.drizzle.jdbc.internal;

/**
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:40:51 PM
 */
public class Utils {
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

    public static int countChars(String str, char c) {
        int index=0;
        int count=0;
        while((index=str.indexOf(c,index))!=-1) {
            count++;
            index++;
        }
        return count;
    }

}
