/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
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

package org.skysql.jdbc.internal.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClient {
    private final URL url;
    private final HttpURLConnection connection;
    public HttpClient(String url) throws IOException {
        this.url = new URL(url);
        connection = (HttpURLConnection) this.url.openConnection();
        
    }

    public String put(InputStream is) throws IOException {
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        OutputStream os = connection.getOutputStream();
        byte [] buffer = new byte[10000];
        while(true) {
            int readBytes = is.read(buffer);
            if(readBytes == -1) {
                break;
            }
            os.write(buffer, 0, readBytes);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String toReturn = br.readLine();
        br.close();
        return toReturn;
    }

    public InputStream get() throws IOException {
        connection.setRequestMethod("GET");

        connection.setDoOutput(true);
        connection.connect();

        return connection.getInputStream(); 
    }
    public void close() {
        connection.disconnect();
    }
}
