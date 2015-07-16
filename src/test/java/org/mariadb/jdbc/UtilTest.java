package org.mariadb.jdbc;

import org.junit.Test;
import org.mariadb.jdbc.internal.common.Utils;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;


public class UtilTest {
    @Test
    public void escape() throws SQLException {
        String[] inputs = new String[] {
                "select {fn timestampdiff(SQL_TSI_HOUR, {fn convert('SQL_', SQL_INTEGER)})}",
                "{call foo({fn now()})}",
                "{?=call foo({fn now()})}",
                "SELECT 'David_' LIKE 'David|_' {escape '|'}",
                "select {fn dayname ({fn abs({fn now()})})}",
                "{d '1997-05-24'}",
                "{d'1997-05-24'}",
                "{t '10:30:29'}",
                "{t'10:30:29'}",
                "{ts '1997-05-24 10:30:29.123'}",
                "{ts'1997-05-24 10:30:29.123'}",
                "'{string data with { or } will not be altered'",
                "--  Also note that you can safely include { and } in comments"
        } ;
        String[] outputs = new String[] {
                "select timestampdiff(HOUR, convert('SQL_', INTEGER))",
                "call foo(now())",
                "?=call foo(now())",
                "SELECT 'David_' LIKE 'David|_' escape '|'",
                "select dayname (abs(now()))",
                "'1997-05-24'",
                "'1997-05-24'",
                "'10:30:29'",
                "'10:30:29'",
                "'1997-05-24 10:30:29.123'" ,
                "'1997-05-24 10:30:29.123'" ,
                "'{string data with { or } will not be altered'",
                "--  Also note that you can safely include { and } in comments"
        };
        for(int i = 0; i < inputs.length; i++)
            assertEquals(Utils.nativeSQL(inputs[i],false), outputs[i]);
    }
}