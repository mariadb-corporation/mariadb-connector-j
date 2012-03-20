package org.skysql.jdbc.internal.mysql;

import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class MySQLQueryLogger {
	private final static Logger log = Logger.getLogger(MySQLQueryLogger.class.getName());
	private boolean explainSlowQueries;
	private boolean autoSlowLog;
	private int	thresholdMillis;
	private int	thresholdNanos;

	public MySQLQueryLogger(Properties info)
	{
		String value = info.getProperty("slowQueryThresholdMillis");
		if (value != null)
		{
			thresholdMillis = Integer.parseInt(value);
		}
		else
		{
			thresholdMillis = -1;
		}
		value = info.getProperty("slowQueryThresholdNanos");
		if (value != null)
		{
			thresholdNanos = Integer.parseInt(value);
		}
		else
		{
			thresholdNanos = -1;
		}
		value = info.getProperty("autoSlowLog");
		if (value != null && value.equalsIgnoreCase("true"))
			autoSlowLog = true;
		else
			autoSlowLog = false;
		value = info.getProperty("explainSlowQueries");
		if (value != null && value.equalsIgnoreCase("true"))
			explainSlowQueries = true;
		else
			explainSlowQueries = false;
	}
	
	public void logQuery()
	{
		if (autoSlowLog)
		{
			
		}
	}
}
