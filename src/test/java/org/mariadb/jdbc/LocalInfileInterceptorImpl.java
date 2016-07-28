package org.mariadb.jdbc;

public class LocalInfileInterceptorImpl implements LocalInfileInterceptor {

    @Override
    public boolean validate(String fileName) {
        return fileName != null && fileName.contains("validateInfile");
    }
}
