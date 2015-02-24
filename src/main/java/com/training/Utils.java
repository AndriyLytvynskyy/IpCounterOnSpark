package com.training;

/**
 * Created by Andrii_Lytvynskyi on 2/12/2015.
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
