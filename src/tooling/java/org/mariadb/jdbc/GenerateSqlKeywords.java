//  SPDX-License-Identifier: LGPL-2.1-or-later
//  Copyright (c) 2012-2014 Monty Program Ab
//  Copyright (c) 2024 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Permit to generate DatabaseMetaData.getSQLKeywords from server source code, reading sql/lex.h, then removing SQL:2003 keywords
 */
public class GenerateSqlKeywords {
    public static final String serverSourcePath = "C:/projects/server";
    public static final String sql2003Keywords = "ADD,ALL,ALLOCATE,ALTER,AND,ANY,ARE,ARRAY,AS,ASENSITIVE,ASYMMETRIC,AT,ATOMIC,AUTHORIZATION,BEGIN,BETWEEN,BIGINT,BINARY,BLOB,BOOLEAN,BOTH,BY,CALL,CALLED,CASCADED,CASE,CAST,CHAR,CHARACTER,CHECK,CLOB,CLOSE,COLLATE,COLUMN,COMMIT,CONDITION,CONNECT,CONSTRAINT,CONTINUE,CORRESPONDING,CREATE,CROSS,CUBE,CURRENT,CURRENT_DATE,CURRENT_DEFAULT_TRANSFORM_GROUP,CURRENT_PATH,CURRENT_ROLE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_TRANSFORM_GROUP_FOR_TYPE,CURRENT_USER,CURSOR,CYCLE,DATE,DAY,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFAULT,DELETE,DEREF,DESCRIBE,DETERMINISTIC,DISCONNECT,DISTINCT,DO,DOUBLE,DROP,DYNAMIC,EACH,ELEMENT,ELSE,ELSEIF,END,ESCAPE,EXCEPT,EXEC,EXECUTE,EXISTS,EXIT,EXTERNAL,FALSE,FETCH,FILTER,FLOAT,FOR,FOREIGN,FREE,FROM,FULL,FUNCTION,GET,GLOBAL,GRANT,GROUP,GROUPING,HANDLER,HAVING,HOLD,HOUR,IDENTITY,IF,IMMEDIATE,IN,INDICATOR,INNER,INOUT,INPUT,INSENSITIVE,INSERT,INT,INTEGER,INTERSECT,INTERVAL,INTO,IS,ITERATE,JOIN,LANGUAGE,LARGE,LATERAL,LEADING,LEAVE,LEFT,LIKE,LOCAL,LOCALTIME,LOCALTIMESTAMP,LOOP,MATCH,MEMBER,MERGE,METHOD,MINUTE,MODIFIES,MODULE,MONTH,MULTISET,NATIONAL,NATURAL,NCHAR,NCLOB,NEW,NO,NONE,NOT,NULL,NUMERIC,OF,OLD,ON,ONLY,OPEN,OR,ORDER,OUT,OUTER,OUTPUT,OVER,OVERLAPS,PARAMETER,PARTITION,PRECISION,PREPARE,PROCEDURE,RANGE,READS,REAL,RECURSIVE,REF,REFERENCES,REFERENCING,RELEASE,REPEAT,RESIGNAL,RESULT,RETURN,RETURNS,REVOKE,RIGHT,ROLLBACK,ROLLUP,ROW,ROWS,SAVEPOINT,SCOPE,SCROLL,SEARCH,SECOND,SELECT,SENSITIVE,SESSION_USER,SET,SIGNAL,SIMILAR,SMALLINT,SOME,SPECIFIC,SPECIFICTYPE,SQL,SQLEXCEPTION,SQLSTATE,SQLWARNING,START,STATIC,SUBMULTISET,SYMMETRIC,SYSTEM,SYSTEM_USER,TABLE,TABLESAMPLE,THEN,TIME,TIMESTAMP,TIMEZONE_HOUR,TIMEZONE_MINUTE,TO,TRAILING,TRANSLATION,TREAT,TRIGGER,TRUE,UNDO,UNION,UNIQUE,UNKNOWN,UNNEST,UNTIL,UPDATE,USER,USING,VALUE,VALUES,VARCHAR,VARYING,WHEN,WHENEVER,WHERE,WHILE,WINDOW,WITH,WITHIN,WITHOUT,YEAR";
    public static void main(String[] args) throws IOException {
        Path path = Paths.get(serverSourcePath);
        if (!Files.exists(path)) throw new IllegalArgumentException("Wrong server source path: " + serverSourcePath);
        List<String> keywords = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(serverSourcePath + "/sql/lex.h"))) {
             boolean firstSymbolLine = false;

             String line = reader.readLine();
             while (line != null) {
                 if (!firstSymbolLine) {
                     if (line.startsWith("SYMBOL symbols[] =")) firstSymbolLine = true;
                 } else {
                    String[] splitVal = line.split("\"");
                    if (splitVal.length > 2) {
                        String keyword = splitVal[1].toUpperCase(Locale.ROOT);
                        if (keyword.charAt(0) >= 'A' && keyword.charAt(0) <= 'Z') {
                            keywords.add(keyword);
                        }
                    }
                }
                line = reader.readLine();
             }
        }
        keywords.sort(String.CASE_INSENSITIVE_ORDER);
        System.out.println("TOTAL:" + String.join(",", keywords));

        List<String> keywordsWithoutSql2003 = new ArrayList<>();
        // remove SQL 2003 keywords
        List<String> sql2003KeyWords = Arrays.asList(sql2003Keywords.split(","));
        for (String key : keywords) {
            if (sql2003KeyWords.contains(key)) {
                System.out.println("removed SQL 2003 keywords " + key);
            } else keywordsWithoutSql2003.add(key);
        }
        System.out.println("TOTAL without SQL2003:" + String.join(",", keywordsWithoutSql2003));
        List<String> completeReservedWords = new ArrayList<>();
        completeReservedWords.addAll(keywordsWithoutSql2003);
        completeReservedWords.addAll(sql2003KeyWords);
        completeReservedWords.sort(String.CASE_INSENSITIVE_ORDER);
        System.out.println("TOTAL with SQL2003:" + String.join(",", completeReservedWords));
    }
}
