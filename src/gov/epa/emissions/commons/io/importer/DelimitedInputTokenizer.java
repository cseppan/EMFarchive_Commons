package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

//FIXME: eliminate me ?
public class DelimitedInputTokenizer {

    public String[] tokensUsingSpace(String line) {
        Pattern p = Pattern.compile("\\s");
        String[] tokens = p.split(line);// split by single whitespace

        List results = new ArrayList();
        for (int i = 0; i < tokens.length; i++) {
            // identify start of quoted text
            if (isStartOfQuotedText(tokens[i])) {
                StringBuffer quoted = new StringBuffer();
                i = squishQuotedTokens(tokens, i, quoted);
                results.add(quoted.toString());

                continue;
            }

            if (tokens[i].length() != 0)// skip comments
                results.add(tokens[i]);
        }

        return (String[]) results.toArray(new String[0]);
    }

    private boolean isStartOfQuotedText(String token) {
        String single = "'(.)*'";
        String startQuoted = "'(.)*^'";

        return !Pattern.matches(single, token) && Pattern.matches(startQuoted, token);
    }

    // squish/collapse the tokens into a single string.
    // First token starts w/ quote and last ends with a quote
    private int squishQuotedTokens(String[] tokens, int index, StringBuffer buffer) {
        buffer.append(tokens[index].substring(1));
        String quote = Character.toString(tokens[index].charAt(0));

        for (index++; (index < tokens.length); index++) {
            if (tokens[index].endsWith(quote)) {// last token
                buffer.append(" " + tokens[index].substring(0, tokens[index].length() - 1));
                break;
            }
            if (tokens[index].length() == 0)// preserve whitespace
                buffer.append(" ");
            else
                buffer.append(" " + tokens[index]);// text
        }
        return index;
    }

}
