package org.mariadb.jdbc;

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
          arr[i] = parseSingleHostAddress(tokens[i]);
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

    static HostAddress parseSingleHostAddress(String s) {
    	 HostAddress result = new HostAddress(); 
    	 if (s.startsWith("[")) {
    		/* IPv6 addresses in URLs are enclosed in square brackets */
          	int ind = s.indexOf(']');
          	result.host = s.substring(1,ind);
          	if (ind != (s.length() -1) && s.charAt(ind + 1) == ':') { 
          		result.port = Integer.parseInt(s.substring(ind+2));
          	}
          }
          else if (s.contains(":")) {
        	  /* Parse host:port */
              String[] hostPort = s.split(":");
              result.host = hostPort[0];
              result.port = Integer.parseInt(hostPort[1]);
          } else {
        	  /* Just host name is given */
              result.host = s;
          }
          return result;
    }
    public static String toString(HostAddress[] addrs) {
        String s="";
        for(int i=0; i < addrs.length; i++) {
        	boolean isIPv6 = addrs[i].host != null && addrs[i].host.contains(":");
        	String host = (isIPv6)?("[" + addrs[i].host + "]"):addrs[i].host;
            s += host + ":" + addrs[i].port;
            if (i < addrs.length -1)
                s += ",";
        }
        return s;
    }
}

