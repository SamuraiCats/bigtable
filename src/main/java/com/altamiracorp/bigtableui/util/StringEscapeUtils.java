package com.altamiracorp.bigtableui.util;

public class StringEscapeUtils {
    /**
     * Unescape any C escape sequences (\n, \r, \\, \ooo, etc) and return the
     * resulting string.
     */
    public static String unescapeCString(String s) {
        if (s.indexOf('\\') < 0) {
            // Fast path: nothing to unescape
            return s;
        }

        StringBuilder sb = new StringBuilder();
        int len = s.length();
        for (int i = 0; i < len; ) {
            char c = s.charAt(i++);
            if (c == '\\' && (i < len)) {
                c = s.charAt(i++);
                switch (c) {
                    case 'a':
                        c = '\007';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'v':
                        c = '\013';
                        break;
                    case '\\':
                        c = '\\';
                        break;
                    case '?':
                        c = '?';
                        break;
                    case '\'':
                        c = '\'';
                        break;
                    case '"':
                        c = '\"';
                        break;

                    default: {
                        if ((c == 'x') && (i < len) && isHex(s.charAt(i))) {
                            // "\xXX"
                            int v = hexValue(s.charAt(i++));
                            if ((i < len) && isHex(s.charAt(i))) {
                                v = v * 16 + hexValue(s.charAt(i++));
                            }
                            c = (char) v;
                        } else if (isOctal(c)) {
                            // "\OOO"
                            int v = (c - '0');
                            if ((i < len) && isOctal(s.charAt(i))) {
                                v = v * 8 + (s.charAt(i++) - '0');
                            }
                            if ((i < len) && isOctal(s.charAt(i))) {
                                v = v * 8 + (s.charAt(i++) - '0');
                            }
                            c = (char) v;
                        } else {
                            // Propagate unknown escape sequences.
                            sb.append('\\');
                        }
                        break;
                    }
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static boolean isOctal(char c) {
        return (c >= '0') && (c <= '7');
    }

    private static boolean isHex(char c) {
        return ((c >= '0') && (c <= '9')) ||
                ((c >= 'a') && (c <= 'f')) ||
                ((c >= 'A') && (c <= 'F'));
    }

    private static int hexValue(char c) {
        if ((c >= '0') && (c <= '9')) {
            return (c - '0');
        } else if ((c >= 'a') && (c <= 'f')) {
            return (c - 'a') + 10;
        } else {
            return (c - 'A') + 10;
        }
    }
}
