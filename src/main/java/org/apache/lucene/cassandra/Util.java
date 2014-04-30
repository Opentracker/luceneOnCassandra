package org.apache.lucene.cassandra;

public class Util {
    
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    
    
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
    
    public static String debugBytesToHex(byte[] bytes) {
        String result = bytesToHex(bytes);
        StringBuffer sb = new StringBuffer();
        if (result.length() > 20) {
            sb.append(result.substring(0, 10));
            sb.append("...");
            sb.append(result.substring(result.length() - 10, result.length()));
        } else {
            sb.append(result);
        }
        
        return sb.toString();
    }
    
    public static String hexToAscii(String hex) {
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < hex.length(); i+=2) {
            String str = hex.substring(i, i+2);
            output.append((char)Integer.parseInt(str, 16));
        }
        return output.toString();
    }
    
    public static String getFileName(CassandraFile file) {
        String abs = file.getAbsolutePath();
        int sep = abs.lastIndexOf("/");
        return abs.substring(sep + 1, abs.length());
    }
    
    public static String getCassandraPath(CassandraFile file) {
        String abs = file.getAbsolutePath();
        int sep = abs.lastIndexOf("/");
        return abs.substring(0, sep) + "/";
    }

    public static String getFileName(File file) {
        String abs = file.getAbsolutePath();
        int sep = abs.lastIndexOf("/");
        return abs.substring(sep + 1, abs.length());
    }
    
    public static String getCassandraPath(File file) {
        String abs = file.getAbsolutePath();
        int sep = abs.lastIndexOf("/");
        return abs.substring(0, sep) + "/";
    }

}
