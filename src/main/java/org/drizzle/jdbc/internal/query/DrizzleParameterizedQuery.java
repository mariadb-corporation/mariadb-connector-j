package org.drizzle.jdbc.internal.query;

import static org.drizzle.jdbc.internal.Utils.countChars;

import java.io.InputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.OutputStream;
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
public class DrizzleParameterizedQuery extends DrizzleQuery {
    private List<ParameterHolder> parameters;
    private final int paramCount;

    public DrizzleParameterizedQuery(String query) {
        super(query);
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
    @Override
    public int length() {
        int length = super.length() - paramCount; // remove the ?s
        for(ParameterHolder param : parameters)
            length+=param.length();
        return length;
    }
    @Override
    public void writeTo(OutputStream os) throws IOException {
        StringReader strReader = new StringReader(query);
        int ch;
        int paramCounter=0;
        while((ch=strReader.read())!=-1) {
            if(ch=='?') {
                parameters.get(paramCounter++).writeTo(os);
            } else {
                os.write(ch);
            }
        }
    }


}
