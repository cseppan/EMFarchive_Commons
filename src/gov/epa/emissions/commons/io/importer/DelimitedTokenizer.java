package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelimitedTokenizer {

    private static final String SINGLE_QUOTED_TEXT = "('(.)*')";

    private static final String DOUBLE_QUOTED_TEXT = "(\"(.)*\")";

    private static final String INLINE_COMMENTS = "!(.)*";

    public String[] doTokenize(String input, String pattern) {
        String completePattern = DOUBLE_QUOTED_TEXT + "|" + SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS + "|" + pattern;

        Pattern p = Pattern.compile(completePattern);
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
}
