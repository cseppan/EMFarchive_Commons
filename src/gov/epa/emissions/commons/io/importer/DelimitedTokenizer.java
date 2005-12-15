package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelimitedTokenizer {

    private Pattern p;

    public DelimitedTokenizer(String pattern) {
        p = Pattern.compile(pattern);
    }

    public String[] doTokenize(String input) {
        Matcher m = p.matcher(input);
        List tokens = new ArrayList();
        while (m.find()) {
            String token = input.substring(m.start(), m.end()).trim();

            if (token.matches(Tokenizer.SINGLE_QUOTED_TEXT) || token.matches(Tokenizer.DOUBLE_QUOTED_TEXT))// quoted
                tokens.add(token.substring(1, token.length() - 1));// strip
            // quotes
            else
                tokens.add(token);
        }

        return (String[]) tokens.toArray(new String[0]);
    }
}
