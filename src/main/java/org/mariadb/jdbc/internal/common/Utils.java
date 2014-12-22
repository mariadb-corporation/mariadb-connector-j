/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:


Copyright (c) 2009-2011, Marcus Eriksson, Jay Pipes
Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/
package org.mariadb.jdbc.internal.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class Utils {


    

    enum LexState
    {
        Normal, /* inside  query */
        String, /* inside string */
        SlashStarComment, /* inside slash-star comment */
        Escape, /* found backslash */
        Parameter, /* parameter placeholder found */
        EOLComment, /* # comment, or // comment, or -- comment */
        Backtick /* found backtick */
    };


     public static List<String> createQueryParts(String queryString, boolean noBackslashEscapes)
     {
          List<String> list = new ArrayList<String>();
          LexState state = LexState.Normal;
          char lastChar= '\0';

          StringBuffer sb = new StringBuffer();

          boolean singleQuotes = false;
          boolean isParam = false;

          char[] query = queryString.toCharArray();

          for (int i = 0; i < query.length; i++)  {
              if (state == LexState.Escape) {
                  sb.append(query[i]);
                  state = LexState.String;
                  continue;
              }

              char c = query[i];
              switch (c)  {
                  case '*':
                      if (state == LexState.Normal && lastChar == '/')
                          state = LexState.SlashStarComment;
                      break;
                  case '/':
                      if (state == LexState.SlashStarComment && lastChar == '*')
                          state = LexState.Normal;
                      else if (state == LexState.Normal && lastChar == '/')
                          state = LexState.EOLComment;
                      break;

                  case '#':
                      if (state == LexState.Normal)
                          state = LexState.EOLComment;
                      break;

                  case '-':
                      if (state == LexState.Normal && lastChar == '-')
                          state = LexState.EOLComment;
                      break;

                  case '\n':
                      if (state == LexState.EOLComment)
                          state = LexState.Normal;
                      break;

                  case '"':
                      if (state == LexState.Normal) {
                          state = LexState.String;
                          singleQuotes = false;
                      }
                      else if (state == LexState.String && !singleQuotes)
                          state = LexState.Normal;
                      break;

                  case '\'':
                      if (state == LexState.Normal) {
                          state = LexState.String;
                          singleQuotes = true;
                      }
                      else if (state == LexState.String && singleQuotes)
                          state = LexState.Normal;
                      break;

                  case '\\':
                      if (noBackslashEscapes)
                          break;
                      if (state == LexState.String)
                          state = LexState.Escape;
                      break;

                  case '?':
                      if (state == LexState.Normal)
                          isParam = true;
                      break;
                  case '`':
                	  if (state == LexState.Backtick) {
                		  state = LexState.Normal;
                	  } else if (state == LexState.Normal) {
                		  state = LexState.Backtick;
                	  }
                	  break;
              }
              lastChar = c;
              if (isParam)  {
                  list.add(sb.toString());
                  sb.setLength(0);
                  isParam = false;
              }
              else  {
                  sb.append(c);
              }

          }
          list.add(sb.toString());
          return list;
     }


    public static String escapeString(String s, boolean noBackslashEscapes) {
    	if (s.indexOf("'") == -1) {
    		if (noBackslashEscapes)
    			return s;
    		if (s.indexOf("\\") == -1)
    			return s;
    	}
    	String escaped = s.replace("'", "''");
    	if(noBackslashEscapes)
    		return escaped;
    	return escaped.replace("\\","\\\\");
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
     * Helper function to replace function parameters in escaped string.
     * 3 functions are handles :
     *  - CONVERT(value, type) , we replace SQL_XXX types with XXX, i.e SQL_INTEGER with INTEGER
     *  - TIMESTAMPDIFF(type, ...) or TIMESTAMPADD(type, ...) , we replace SQL_TSI_XXX in type with XXX, i.e
     *    SQL_TSI_HOUR with HOUR
     * @param s - input string
     * @return unescaped string
     */
    public static String replaceFunctionParameter(String s)  {

        if (!s.contains("SQL_"))
            return s;

        char[] input = s.toCharArray();
        StringBuffer sb = new StringBuffer();
        int i;
        for (i = 0; i < input.length; i++)  {
            if (input[i] != ' ')
                break;
        }

        for (; ((input[i] >= 'a' && i <= 'z') || (input[i] >= 'A' && input[i] <= 'Z')) && i < input.length; i++)                {
            sb.append(input[i]);
        }
        String func = sb.toString().toLowerCase();

        if ( func.equals("convert") || func.equals("timestampdiff") || func.equals("timestampadd")) {
            String paramPrefix;

            if (func.equals("timestampdiff") || func.equals("timestampadd")) {
                // Skip to first parameter
                for (; i < input.length; i++) {
                    if (!Character.isWhitespace(input[i]) && input[i] != '(')
                        break;
                }
                if (i == input.length)
                    return new String(input);


                if (i >= input.length - 8)
                    return new String(input);
                paramPrefix = new String(input, i, 8);
                if (paramPrefix.equals("SQL_TSI_"))
                    return new String(input, 0, i) + new String(input, i + 8, input.length - (i+8));
                return new String(input);
            }

            // Handle "convert(value, type)" case
            // extract last parameter, after the last ','
            int lastCommaIndex = s.lastIndexOf(',');

            for (i = lastCommaIndex + 1; i < input.length; i++) {
                if (!Character.isWhitespace(input[i]))
                  break;
            }
            if (i>= input.length - 4)
                return new String(input);
             paramPrefix = new String(input, i, 4);
            if (paramPrefix.equals("SQL_"))
                return new String(input, 0, i) + new String(input, i+4 , input.length - (i+4));

        }
        return new String(input);
     }

    private static String resolveEscapes(String escaped, boolean noBackslashEscapes) throws SQLException{
        if(escaped.charAt(0) != '{' || escaped.charAt(escaped.length()-1) != '}')
            throw new SQLException("unexpected escaped string");
        int endIndex = escaped.length()-1;
        String escapedLower = escaped.toLowerCase();
        if (escaped.startsWith("{fn ")) {
            String resolvedParams = replaceFunctionParameter(escaped.substring(4,endIndex));
            return nativeSQL(resolvedParams, noBackslashEscapes);
        }
        else if(escapedLower.startsWith("{oj ")) {
            // Outer join
        	// the server supports "oj" in any case, even "oJ"
            return nativeSQL(escaped.substring(4, endIndex), noBackslashEscapes);
        }
        else if(escaped.startsWith("{d "))  {
            // date literal
            return escaped.substring(3, endIndex);
        }
        else if(escaped.startsWith("{t ")) {
            // time literal
            return escaped.substring(3, endIndex);
        }
        else if (escaped.startsWith("{ts ")) {
            //timestamp literal
            return escaped.substring(4, endIndex);
        }
        else if(escaped.startsWith("{d'"))  {
            // date literal, no space 
            return escaped.substring(2, endIndex);
        }
        else if(escaped.startsWith("{t'")) {
            // time literal
            return escaped.substring(2, endIndex);
        }
        else if (escaped.startsWith("{ts'")) {
            //timestamp literal
            return escaped.substring(3, endIndex);
        }
        else if (escaped.startsWith("{call ") || escaped.startsWith("{CALL ")) {
            // We support uppercase "{CALL" only because Connector/J supports it. It is not in the JDBC spec.

            return  nativeSQL(escaped.substring(1, endIndex), noBackslashEscapes);
        }
        else if (escaped.startsWith("{escape ")) {
            return  escaped.substring(1, endIndex);
        }
        else if (escaped.startsWith("{?")) {
           // likely ?=call(...)
           return nativeSQL(escaped.substring(1, endIndex), noBackslashEscapes);
        } else if (escaped.startsWith("{ ")) {
            // Spaces before keyword, this is not JDBC compliant, however some it works in some drivers,
            // so we support it, too
            for(int i = 2; i < escaped.length(); i++) {
                if (!Character.isWhitespace(escaped.charAt(i))) {
                   return resolveEscapes("{" + escaped.substring(i), noBackslashEscapes);
                }
            }
        }
        throw new SQLException("unknown escape sequence " + escaped);
    }


    public static String nativeSQL(String sql, boolean noBackslashEscapes) throws SQLException{
        if (sql.indexOf('{') == -1)
            return sql;

        StringBuffer escapeSequenceBuf = new StringBuffer();
        StringBuffer sqlBuffer = new StringBuffer();

        char [] a = sql.toCharArray();
        char lastChar = 0;
        boolean inQuote = false;
        char quoteChar = 0;
        boolean inComment = false;
        boolean isSlashSlashComment = false;
        int inEscapeSeq  = 0;

        for(int i = 0 ; i< a.length; i++) {
            char c = a[i];
            if (lastChar == '\\' && !noBackslashEscapes) {
                sqlBuffer.append(c);
                continue;
            }

            switch(c) {
                case '\'':
                case '"':
                    if (!inComment) {
                        if(inQuote) {
                            if (quoteChar == c) {
                                inQuote = false;
                            }
                        } else {
                            inQuote = true;
                            quoteChar = c;
                        }
                    }
                    break;

                case '*':
                    if (!inQuote && !inComment) {
                        if(lastChar == '/') {
                            inComment = true;
                            isSlashSlashComment = false;
                        }
                    }
                    break;
                case '/':
                case '-':
                    if (!inQuote) {
                        if (inComment) {
                            if (lastChar == '*' && !isSlashSlashComment) {
                                inComment = false;
                            }
                            else if (lastChar == c && isSlashSlashComment) {
                                inComment = false;
                            }
                        }
                        else {
                            if(lastChar == c) {
                                inComment = true;
                                isSlashSlashComment = true;
                            }
                            else if (lastChar == '*') {
                                inComment = true;
                                isSlashSlashComment = false;
                            }
                        }
                    }
                    break;
                case 'S':
                    // skip SQL_xxx and SQL_TSI_xxx in functions
                    // This would convert e.g SQL_INTEGER => INTEGER, SQL_TSI_HOUR=>HOUR

                    if (!inQuote && !inComment && inEscapeSeq > 0) {
                       if (i + 4 < a.length && a[i+1] == 'Q' && a[i+2] == 'L' && a[i+3] == 'L' && a[i+4] == '_') {
                           if(i+8 < a.length && a[i+5] == 'T' && a[i+6] == 'S' && a[i+7] == 'I' && a[i+8] == '_') {
                               i += 8;
                               continue;
                           }
                           i += 4;
                           continue;
                       }
                    }
                    break;
                case '\n':
                    if (inComment && isSlashSlashComment) {
                        // slash-slash and dash-dash comments ends with the end of line
                        inComment = false;
                    }
                    break;
                case '{':
                    if (!inQuote && ! inComment) {
                        inEscapeSeq++;
                    }
                    break;

                case '}':
                    if (!inQuote && ! inComment) {
                        inEscapeSeq--;
                        if (inEscapeSeq == 0) {
                            escapeSequenceBuf.append(c);
                            sqlBuffer.append(resolveEscapes(escapeSequenceBuf.toString(), noBackslashEscapes));
                            escapeSequenceBuf.setLength(0);
                            continue;
                        }
                    }


            }
            lastChar = c;
            if(inEscapeSeq > 0) {
                escapeSequenceBuf.append(c);
            } else {
                sqlBuffer.append(c);
            }
        }
        if (inEscapeSeq > 0)
            throw new SQLException("Invalid escape sequence , missing closing '}' character in '" + sqlBuffer);
        return sqlBuffer.toString();
    }
    
    /**
     * Adds the parsed parameter to the properties object.
     * 
     * @param parameter a key=value pair
     * @param info the properties object
     */
    public static void setUrlParameter(String parameter, Properties info) {
    	int pos = parameter.indexOf('=');
        if (pos == -1)  {
            throw new IllegalArgumentException("Invalid connection URL, expected key=value pairs, found " + parameter);
        }
        info.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
    }
    
    /**
     * Parses the parameters string and sets the corresponding properties in the properties object.
     * 
     * @param urlParameters the parameters string
     * @param info the properties object
     */
    public static void setUrlParameters(String urlParameters, Properties info) {
    	String [] parameters = urlParameters.split("&");
    	for(String parameter : parameters) {
    		setUrlParameter(parameter, info);
    	}
    }
}