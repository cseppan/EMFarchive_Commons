package gov.epa.emissions.commons.util;

public class CustomStringTools {

    public static String replaceNoneLetterDigit(String string, char substitute) {
        for (int i = 0; i < string.length(); i++) {
            if (!Character.isLetterOrDigit(string.charAt(i))) {
                string = string.replace(string.charAt(i), substitute);
            }
        }

        return string;
    }
    
    public static String escapeBackSlash4jdbc(String col) {
        return col.replaceAll("\\\\", "\\\\\\\\");
    }
    
    public static String escapeBackSlash(String string) {
        return string.replaceAll("\\\\", "\\\\");
    }
}
