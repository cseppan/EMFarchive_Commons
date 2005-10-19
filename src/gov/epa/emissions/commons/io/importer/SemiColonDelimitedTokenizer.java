package gov.epa.emissions.commons.io.importer;

public class SemiColonDelimitedTokenizer implements Tokenizer {

    private static final String ANY_CHAR_EXCEPT_SEMICOLON = "[^;\\s]+";

    private DelimitedTokenizer delegate;

    public SemiColonDelimitedTokenizer() {
        delegate = new DelimitedTokenizer();
    }

    public String[] tokens(String input) {
        return delegate.doTokenize(input, ANY_CHAR_EXCEPT_SEMICOLON);
    }

}
