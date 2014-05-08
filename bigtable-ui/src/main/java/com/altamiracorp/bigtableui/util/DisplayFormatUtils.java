package com.altamiracorp.bigtableui.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Utility class for creating formatted display values that are human-readable
 */
public class DisplayFormatUtils {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.#######E00");

    /**
     * Creates a formatted readable hex string for the provided bytes
     * <pre>
     * Example: {@code "\xA2\xBE"}
     * </pre>
     *
     * @param bytes The bytes to format
     * @return The formatted hex string
     */
    public static String generateHexString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("\\x%02X", b));
        }

        return sb.toString();
    }

    /**
     * Creates a formmatted readable string for the provided number value
     * <pre>
     * Example: 1399497863341 => {@code "1,399,497,863,341"}
     * </pre>
     *
     * @param value The numeric value to format
     * @return The formatted string
     */
    public static String generateFormattedNumber(final Number value) {
        return NumberFormat.getIntegerInstance().format(value);
    }

    /**
     * Creates a formmatted decimal string for the provided number value
     * <pre>
     * Example: 123.87442333 => {@code "1.2387442E02"}
     * </pre>
     *
     * @param value The numeric value to format
     * @return The formatted string
     */
    public static String generateFormattedDecimal(final Number value) {
        return DECIMAL_FORMAT.format(value);
    }
}
