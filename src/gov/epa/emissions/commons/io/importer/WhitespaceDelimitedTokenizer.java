package gov.epa.emissions.commons.io.importer;

public class WhitespaceDelimitedTokenizer implements Tokenizer {

    private static final String ANY_CHAR_EXCEPT_WHITESPACE = "(([\\S]+))";
    private DelimitedTokenizer delegate;

    public WhitespaceDelimitedTokenizer() {
        delegate = new DelimitedTokenizer();
    }

    // whitespace includes space & tabs
    public String[] tokens(String input) {
        return delegate.doTokenize(input, ANY_CHAR_EXCEPT_WHITESPACE);
    }

}
