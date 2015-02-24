package com.training;

/**
 * Utils class (toInteger)
 */
public final class Utils {
    public static int toInteger(String s) {
        int result;
        try {
            result = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
        return result;
    }
}
