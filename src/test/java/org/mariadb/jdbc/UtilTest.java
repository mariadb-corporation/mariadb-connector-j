package org.mariadb.jdbc;

import org.junit.Test;
import org.mariadb.jdbc.internal.common.Utils;

import java.sql.SQLException;

import static junit.framework.Assert.assertEquals;
import static org.mariadb.jdbc.internal.common.Utils.countChars;


public class UtilTest {
    @Test
    public void testCountChars() {
        String test = "aaa?bbcc??xx?";
        assertEquals(4,countChars(test,'?'));
    }
    @Test

    public void testEscapeProcessing() throws Exception {
        String results = "select dayname (abs(now())),   -- Today    \n"
                + "           '1997-05-24',  -- a date                    \n"
                + "           '10:30:29',  -- a time                     \n"
                + "           '1997-05-24 10:30:29.123', -- a timestamp  \n"
                + "          '{string data with { or } will not be altered'   \n"
                + "--  Also note that you can safely include { and } in comments";

        String exSql = "select {fn dayname ({fn abs({fn now()})})},   -- Today    \n"
                + "           {d '1997-05-24'},  -- a date                    \n"
                + "           {t '10:30:29'},  -- a time                     \n"
                + "           {ts '1997-05-24 10:30:29.123'}, -- a timestamp  \n"
                + "          '{string data with { or } will not be altered'   \n"
                + "--  Also note that you can safely include { and } in comments";

        String s = Utils.nativeSQL(exSql, false);
        assertEquals(results, s);
    }
    @Test
    public void escape2() throws SQLException {
        // the query is nonsensical, but makes a good test case for handling SQL_{TSI_}? type modifiers
        String in = "select {fn timestampdiff(SQL_TSI_HOUR, {fn convert('SQL_', SQL_INTEGER)})}";
        String out = Utils.nativeSQL(in, false);
        assertEquals(out, "select timestampdiff(HOUR, convert('SQL_', INTEGER))");
        in = "{call foo({fn now()})}";
        out = Utils.nativeSQL(in, false);
        assertEquals(out, "call foo(now())");
        in = "{?=call foo({fn now()})}";
        out = Utils.nativeSQL(in, false);
        assertEquals(out, "?=call foo(now())");
        in = "SELECT 'David_' LIKE 'David|_' {escape '|'}";
        out = Utils.nativeSQL(in, false);
        assertEquals(out, "SELECT 'David_' LIKE 'David|_' escape '|'");
    }
}