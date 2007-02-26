package gov.epa.emissions.commons.io.importer;

public class PipeDelimitedTokenizer implements Tokenizer {
    private static final String ANY_CHAR_EXCEPT_BAR = "[^|]+";

    private DelimitedTokenizer delegate;
    
    private String pattern;
    
    private int numOfDelimiter;
    
    private boolean initialized = false;

    public PipeDelimitedTokenizer() {
        pattern = DOUBLE_QUOTED_TEXT + "|" +  SINGLE_QUOTED_TEXT + "|" + INLINE_COMMENTS +"|"+ ANY_CHAR_EXCEPT_BAR;
        delegate = new DelimitedTokenizer(pattern);
    }

    public String[] tokens(String input) throws ImporterException {
        String[] tokens = delegate.doTokenize(input);
        
        if (!initialized) {
            numOfDelimiter = tokens.length;
            initialized = true;
            return tokens;
        }
            
        if (initialized && tokens.length != numOfDelimiter) {
            throw new ImporterException("Could not find " + --numOfDelimiter + " of \'|\' delimiters on the line.");
        }
        
        return tokens;
    }

}
