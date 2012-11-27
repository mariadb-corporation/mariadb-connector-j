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

package org.mariadb.jdbc.internal.common.query.parameters;

import org.mariadb.jdbc.internal.common.HttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Holds a blobstreaming parameter.
 *
 * protocol works like this when uploading a stream:
 * 1. upload stream to http://<server>:<port>/<schema>/
 * 2. execute the insert using the blob reference returned
 *
 */
public class BlobStreamingParameter implements ParameterHolder {
    private String blobReference=null;
    private final InputStream blobStream;
    private final HttpClient httpClient;
    /**
     * Create a new StreamParameter.
     *
     * @param is         the input stream to create the parameter from
     */
    public BlobStreamingParameter(final InputStream is, String host, String port, String schema) throws IOException {
        this.blobStream = is;
        httpClient = new HttpClient("http://"+host+":"+port+"/"+schema);

    }

    /**
     * Writes the parameter to an outputstream.
     *
     * @param os the outputstream to write to
     * @throws java.io.IOException if we cannot write to the stream
     */
    public final void writeTo(final OutputStream os) throws IOException {
        os.write(blobReference.getBytes());
    }
}
