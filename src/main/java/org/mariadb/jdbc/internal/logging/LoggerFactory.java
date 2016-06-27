package org.mariadb.jdbc.internal.logging;

import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class LoggerFactory {
    public static Boolean hasToLog = null;
    public static final Logger NO_LOGGER = new NoLogger();

    public static Class loggerClass = null;
    public static Method method = null;

    /**
     * Initialize factory.
     * @param mustLog indicate if must initiate Slf4j logger
     * @throws QueryException if Slf4j dependency is not present.
     */
    public static void init(boolean mustLog) throws QueryException {
        if (hasToLog == null || hasToLog.booleanValue() != mustLog) {
            if (mustLog) {
                try {
                    hasToLog = Boolean.TRUE;
                    loggerClass = Class.forName("org.slf4j.LoggerFactory");
                    method = loggerClass.getMethod("getLogger", Class.class);
                } catch (ClassNotFoundException classNotFound) {
                    throw new QueryException("Logging cannot be activated, missing slf4j dependency");
                } catch (NoSuchMethodException classNotFound) {
                    throw new QueryException("Logging cannot be activated, missing slf4j dependency");
                }
            } else {
                hasToLog = Boolean.FALSE;
            }

        }
    }

    /**
     * Initialize logger.
     * @param clazz initiator class
     * @return logger
     */
    public static Logger getLogger(Class<?> clazz) {
        if (hasToLog) {
            try {
                return new Slf4JLogger((org.slf4j.Logger) method.invoke(loggerClass, clazz));
            } catch (IllegalAccessException illegalAccess) {
                return null;
            } catch (InvocationTargetException invocationException) {
                return null;
            }
        } else {
            return NO_LOGGER;
        }
    }

}
