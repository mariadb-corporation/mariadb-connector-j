package org.drizzle.jdbc;

import org.junit.Test;
import static org.drizzle.jdbc.internal.common.Utils.countChars;
import static junit.framework.Assert.assertEquals;

/**
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 9:19:24 PM
 */
public class UtilTest {
    @Test
    public void testCountChars() {
        String test = "aaa?bbcc??xx?";
        assertEquals(4,countChars(test,'?'));
    }


}