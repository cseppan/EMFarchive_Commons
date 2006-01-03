package gov.epa.emissions.commons.io.importer;

public class WhitespaceDelimitedTokenizer implements Tokenizer {

    private static final String ANY_CHAR_EXCEPT_WHITESPACE = "[\\S]+";
    private DelimitedTokenizer delegate;

    public WhitespaceDelimitedTokenizer() {
        String pattern = INLINE_COMMENTS +"|"+ANY_CHAR_EXCEPT_WHITESPACE;
        delegate = new DelimitedTokenizer(pattern);
    }

    // whitespace includes space & tabs
    public String[] tokens(String input) {
        return delegate.doTokenize(input);
    }

}
