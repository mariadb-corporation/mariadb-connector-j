package org.skysql.jdbc;

public class HostAddress {
    public String host;
    public int port;

    /**
     * parse - parse server addresses from the URL fragment
     * @param spec - list of endpoints in one of the forms
     *    1 - host1,....,hostN:port (missing port default to MySQL default 3306
     *    2 - host:port,...,host:port
     * @return   parsed endpoints
     */
    public static HostAddress[] parse(String spec) {
        String[] tokens = spec.split(",");
        HostAddress[] arr = new HostAddress[tokens.length];

        for (int i=0; i < tokens.length; i++) {
            String t = tokens[i];
            if (t.contains(":")) {
                String[] hostPort = t.split(":");
                arr[i].host = hostPort[0];
                arr[i].port = Integer.parseInt(hostPort[1]);
            } else {
                arr[i].host = t;
            }
        }

        int  defaultPort = arr[arr.length-1].port;
        if (defaultPort == 0) {
            defaultPort = 3306;
        }

        for (int i = 0; i < arr.length; i++) {
            if (arr[i].port == 0) {
                arr[i].port = defaultPort;
            }
        }
        return arr;
    }
}

