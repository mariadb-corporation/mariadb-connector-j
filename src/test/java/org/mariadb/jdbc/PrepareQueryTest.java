package org.mariadb.jdbc;

import org.junit.Test;
import org.mariadb.jdbc.internal.common.Utils;

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
}
