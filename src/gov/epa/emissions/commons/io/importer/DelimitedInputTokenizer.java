package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelimitedInputTokenizer {

    private static final String ANY_CHAR_EXCEPT_WHITESPACE = "(([\\S]+))";

    private static final String ANY_CHAR_EXCEPT_COMMA = "[^,\\s]+";

    private static final String ANY_CHAR_EXCEPT_SEMICOLON = "[^;\\s]+";

    private static final String ANY_CHAR_EXCEPT_SEMICOLON2 = "[^;\\s]+";

    private static final String SINGLE_QUOTED_TEXT = "('(.)*')";

    private static final String DOUBLE_QUOTED_TEXT = "(\"(.)*\")";

    private static final String INLINE_COMMENTS = "!(.)*";

    // whitespace includes space & tabs
    public String[] tokensWhitepaceDelimited(String input) {
        String pattern = DOUBLE_QUOTED_TEXT + "|" + SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS + "|"
                + ANY_CHAR_EXCEPT_WHITESPACE;
        return doTokenize(input, pattern);
    }

    public String[] tokensCommaDelimited(String input) {
        String pattern = DOUBLE_QUOTED_TEXT + "|" + SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS + "|"
                + ANY_CHAR_EXCEPT_COMMA;
        return doTokenize(input, pattern);
    }

    public String[] tokensSemiColonDelimited(String input) {
        String pattern = DOUBLE_QUOTED_TEXT + "|" + SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS + "|"
                + ANY_CHAR_EXCEPT_SEMICOLON;
        return doTokenize(input, pattern);
    }

    public String[] tokensSemiColonDelimited2(String input) {
        String pattern = DOUBLE_QUOTED_TEXT + "|" + SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS + "|"
                + ANY_CHAR_EXCEPT_SEMICOLON2;
        return doTokenize(input, pattern);
    }

    private String[] doTokenize(String input, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(input);

        List tokens = new ArrayList();
        while (m.find()) {
            String token = input.substring(m.start(), m.end()).trim();

            if (token.matches(SINGLE_QUOTED_TEXT) || token.matches(DOUBLE_QUOTED_TEXT))// quoted
                tokens.add(token.substring(1, token.length() - 1));// strip
            // quotes
            else
                tokens.add(token);
        }

        return (String[]) tokens.toArray(new String[0]);
    }

    public String[] tokens(String input, int minimumTokens) {
        String[] commaDelimited = tokensCommaDelimited(input);
        if (commaDelimited.length >= minimumTokens)
            return commaDelimited;

        String[] semiColonDelimited = tokensSemiColonDelimited(input);
        if (semiColonDelimited.length >= minimumTokens) {
            System.out.println("semi-colon delimited");
            return semiColonDelimited;
        }

        return null;
    }
}
