package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.query.DrizzleParameterizedQuery;
import org.drizzle.jdbc.internal.common.query.IllegalParameterException;
import org.drizzle.jdbc.internal.common.query.parameters.StringParameter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Mar 28, 2010 Time: 2:46:20 PM To change this template use File |
 * Settings | File Templates.
 */
public class PrepareQueryTest {

    @Test
    public void testSplit() {
        for(String s : Utils.createQueryParts("aaa ? bbb")) {
            System.out.println(s.getBytes().length);
        }
    }
    @Test
    public void stripQueryUTF() {
        String jaString = "\u65e5\u672c\u8a9e\u6587\u5b57\u5217";
        assertEquals(jaString,Utils.stripQuery(jaString));

    }

    @Test
    public void paramQueryTest() throws IllegalParameterException, IOException, QueryException {
       /* DrizzleParameterizedQuery dpq = new DrizzleParameterizedQuery("SELECT * FROM ABC WHERE ID=?");
        dpq.setParameter(0,new StringParameter("22"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long sumtot = 0;
        for(int k=0; k<1000;k++) {
            long starttime = System.nanoTime();
            for(int i=0;i<10000;i++) {
                dpq.length();
                dpq.writeTo(baos);
            }
            //System.out.println(System.nanoTime() - starttime);
            sumtot +=     System.nanoTime() - starttime;
            baos.reset();
        }
        System.out.println(sumtot / 1000);*/
    }
}
