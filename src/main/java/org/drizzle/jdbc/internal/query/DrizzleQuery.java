package org.drizzle.jdbc.internal.query;

import static org.drizzle.jdbc.internal.Utils.countChars;

import java.io.InputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 18, 2009
 * Time: 10:13:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleQuery {
    private final String query;
    private List<ParameterHolder> parameters;
    private final int paramCount;
    public DrizzleQuery(String query) {
        this.query=query;
        this.paramCount=countChars(query,'?');
        parameters=new ArrayList<ParameterHolder>(paramCount);
    }

    public void setParameter(int position, ParameterHolder parameter) throws SQLException {
        if(position>=0 && position<paramCount)
            this.parameters.add(position,parameter);
        else
            throw new SQLException("No '?' on that position");
    }

    public void clearParameters() {
        this.parameters.clear();
    }

    public int length() {
        int length = query.length() - paramCount; // remove the ?s
        for(ParameterHolder param : parameters)
            length+=param.length();
        return length;
    }

    public QueryReader getQueryReader() {
        return new QueryReader();
    }


    public class QueryReader extends InputStream {
        private int parameterIndex=0;
        private StringReader strReader;
        private int currentParameterPointer=0;
        private ParameterHolder parameter=null;
        public QueryReader() {
            strReader = new StringReader(query);
        }
        /**
         * Reads the next byte of data from the input stream. The value byte is
         * returned as an <code>int</code> in the range <code>0</code> to
         * <code>255</code>. If no byte is available because the end of the stream
         * has been reached, the value <code>-1</code> is returned. This method
         * blocks until input data is available, the end of the stream is detected,
         * or an exception is thrown.
         * <p/>
         * <p> A subclass must provide an implementation of this method.
         *
         * @return the next byte of data, or <code>-1</code> if the end of the
         *         stream is reached.
         * @throws java.io.IOException if an I/O error occurs.
         */
        public int read() throws IOException {
            if(parameter==null) {
                int nextChar =strReader.read();
                if(nextChar == '?') {
                    parameter=parameters.get(parameterIndex++);
                } else {
                    return nextChar;
                }
            }
            if(currentParameterPointer < parameter.length()) {
                currentParameterPointer++;
                return parameter.read();
            } else {
                currentParameterPointer=0;
                parameter=null;
                return strReader.read();
            }
        }
    }

}
