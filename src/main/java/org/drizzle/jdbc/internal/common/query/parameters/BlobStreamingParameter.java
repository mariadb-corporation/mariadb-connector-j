/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */
package org.drizzle.jdbc.internal.common.query.parameters;

import org.drizzle.jdbc.internal.common.HttpClient;

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
    public final int writeTo(final OutputStream os,int offset, int maxWriteSize) throws IOException {
        int bytesToWrite = Math.min(blobReference.getBytes().length - offset, maxWriteSize);
        os.write(blobReference.getBytes(), offset, blobReference.getBytes().length);
        return bytesToWrite;
    }

    /**
     * Returns the length of the parameter - this is the length of the blob reference
     *
     * Since we don't know the length of the reference before it has been uploaded, it is uploaded in this method.
     *
     * @return the length of the blob reference
     */
    public final long length() throws IOException {
        blobReference = "\""+httpClient.put(blobStream)+"\"";
        return blobReference.getBytes().length;
    }
}
