package gov.epa.emissions.commons.io.importer;

public class SemiColonDelimitedTokenizer implements Tokenizer {

    private static final String ANY_CHAR_EXCEPT_SEMICOLON = "[^;]+";

    private DelimitedTokenizer delegate;

    public SemiColonDelimitedTokenizer() {
        String pattern = ANY_CHAR_EXCEPT_SEMICOLON + "|" + INLINE_COMMENTS;
        delegate = new DelimitedTokenizer(pattern);
    }

    public String[] tokens(String input) {
        return delegate.doTokenize(padding(input));
    }

    private String padding(String input) {
        input = input.trim();
        if (input.startsWith(";"))
            input = ";" + input;
        if (input.endsWith(";"))
            input = input + ";";
        input = input.replaceAll(";;", "; ;");
        input = input.replaceAll(";;", "; ;");// called for second time if odd number of delimiter present

        return input;
    }

}
