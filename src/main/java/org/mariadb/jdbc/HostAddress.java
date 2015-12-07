package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.util.constant.ParameterConstant;
import org.mariadb.jdbc.internal.util.constant.HaMode;

import java.util.ArrayList;
import java.util.List;

public class HostAddress {
    public String host;
    public int port;
    public String type = null;


    public HostAddress() {
    }

    /**
     * Constructor. type is master.
     * @param host host
     * @param port port
     */
    public HostAddress(String host, int port) {
        this.host = host;
        this.port = port;
        this.type = ParameterConstant.TYPE_MASTER;
    }

    /**
     * Constructor.
     * @param host host
     * @param port port
     * @param type type
     */
    public HostAddress(String host, int port, String type) {
        this.host = host;
        this.port = port;
        this.type = type;
    }

    /**
     * parse - parse server addresses from the URL fragment
     *
     * @param spec   - list of endpoints in one of the forms
     *               1 - host1,....,hostN:port (missing port default to MySQL default 3306
     *               2 - host:port,...,host:port
     * @param haMode High availability mode
     * @return parsed endpoints
     */
    public static List<HostAddress> parse(String spec, HaMode haMode) {
        if (spec == null) {
            throw new IllegalArgumentException("Invalid connection URL, host address must not be empty ");
        }
        if ("".equals(spec)) {
            return new ArrayList<>(0);
        }
        String[] tokens = spec.trim().split(",");
        List<HostAddress> arr = new ArrayList<>(tokens.length);

        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].startsWith("address=")) {
                arr.add(parseParameterHostAddress(tokens[i]));
            } else {
                arr.add(parseSimpleHostAddress(tokens[i]));
            }
        }

        int defaultPort = arr.get(arr.size() - 1).port;
        if (defaultPort == 0) {
            defaultPort = 3306;
        }

        for (int i = 0; i < arr.size(); i++) {
            if (haMode == HaMode.REPLICATION) {
                if (i == 0 && arr.get(i).type == null) {
                    arr.get(i).type = ParameterConstant.TYPE_MASTER;
                } else if (i != 0 && arr.get(i).type == null) {
                    arr.get(i).type = ParameterConstant.TYPE_SLAVE;
                }
            }
            if (arr.get(i).port == 0) {
                arr.get(i).port = defaultPort;
            }
        }
        return arr;
    }

    static HostAddress parseSimpleHostAddress(String str) {
        HostAddress result = new HostAddress();
        if (str.charAt(0) == '[') {
            /* IPv6 addresses in URLs are enclosed in square brackets */
            int ind = str.indexOf(']');
            result.host = str.substring(1, ind);
            if (ind != (str.length() - 1) && str.charAt(ind + 1) == ':') {
                result.port = Integer.parseInt(str.substring(ind + 2));
            }
        } else if (str.contains(":")) {
              /* Parse host:port */
            String[] hostPort = str.split(":");
            result.host = hostPort[0];
            result.port = Integer.parseInt(hostPort[1]);
        } else {
              /* Just host name is given */
            result.host = str;
        }
        return result;
    }

    static HostAddress parseParameterHostAddress(String str) {
        HostAddress result = new HostAddress();
        String[] array = str.split("(?=\\()|(?<=\\))");
        for (int i = 1; i < array.length; i++) {
            String[] token = array[i].replace("(", "").replace(")", "").trim().split("=");
            if (token.length != 2) {
                throw new IllegalArgumentException("Invalid connection URL, expected key=value pairs, found " + array[i]);
            }
            String key = token[0].toLowerCase();
            String value = token[1].toLowerCase();
            if (key.equals("host")) {
                result.host = value.replace("[", "").replace("]", "");
            } else if (key.equals("port")) {
                result.port = Integer.parseInt(value);
            } else if (key.equals("type")
                    && (value.equals(ParameterConstant.TYPE_MASTER) || value.equals(ParameterConstant.TYPE_SLAVE))) {
                result.type = value;
            }
        }
        return result;
    }

    /**
     * ToString implementation of addresses.
     * @param addrs address list
     * @return String value
     */
    public static String toString(List<HostAddress> addrs) {
        String str = "";
        for (int i = 0; i < addrs.size(); i++) {
            if (addrs.get(i).type != null) {
                str += "address=(host=" + addrs.get(i).host + ")(port=" + addrs.get(i).port + ")(type=" + addrs.get(i).type + ")";
            } else {
                boolean isIPv6 = addrs.get(i).host != null && addrs.get(i).host.contains(":");
                String host = (isIPv6) ? ("[" + addrs.get(i).host + "]") : addrs.get(i).host;
                str += host + ":" + addrs.get(i).port;
            }
            if (i < addrs.size() - 1) {
                str += ",";
            }
        }
        return str;
    }

    /**
     * ToString implementation of addresses.
     * @param addrs address array
     * @return String value
     */
    public static String toString(HostAddress[] addrs) {
        String str = "";
        for (int i = 0; i < addrs.length; i++) {
            if (addrs[i].type != null) {
                str += "address=(host=" + addrs[i].host + ")(port=" + addrs[i].port + ")(type=" + addrs[i].type + ")";
            } else {
                boolean isIPv6 = addrs[i].host != null && addrs[i].host.contains(":");
                String host = (isIPv6) ? ("[" + addrs[i].host + "]") : addrs[i].host;
                str += host + ":" + addrs[i].port;
            }
            if (i < addrs.length - 1) {
                str += ",";
            }
        }
        return str;
    }

    @Override
    public String toString() {
        return "HostAddress{"
                + "host='" + host + '\''
                + ", port=" + port
                + ", type='" + type + '\''
                + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        HostAddress that = (HostAddress) obj;

        if (port != that.port) {
            return false;
        }
        if (host != null ? !host.equals(that.host) : that.host != null) {
            return false;
        }
        return !(type != null ? !type.equals(that.type) : that.type != null);

    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }


}

