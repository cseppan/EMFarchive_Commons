package gov.epa.emissions.commons.io.importer;

public class PipeDelimitedTokenizer implements Tokenizer {
    private static final String ANY_CHAR_EXCEPT_BAR = "[^|]+";

    private DelimitedTokenizer delegate;
    
    private String pattern;

    public PipeDelimitedTokenizer() {
        pattern = DOUBLE_QUOTED_TEXT + "|" +  SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS +"|"+ ANY_CHAR_EXCEPT_BAR;
        delegate = new DelimitedTokenizer(pattern);
    }

    public String[] tokens(String input) {
        return delegate.doTokenize(input);
    }

}
