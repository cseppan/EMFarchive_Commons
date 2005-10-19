package gov.epa.emissions.commons.io.importer;

public class CommaDelimitedTokenizer implements Tokenizer {

    private static final String ANY_CHAR_EXCEPT_COMMA = "[^,\\s]+";

    private DelimitedTokenizer delegate;

    public CommaDelimitedTokenizer() {
        delegate = new DelimitedTokenizer();
    }

    public String[] tokens(String input) {
        return delegate.doTokenize(input, ANY_CHAR_EXCEPT_COMMA);
    }

}
