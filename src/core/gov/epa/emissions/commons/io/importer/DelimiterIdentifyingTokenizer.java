package gov.epa.emissions.commons.io.importer;

public class DelimiterIdentifyingTokenizer implements Tokenizer {

    private int minTokens;

    private Tokenizer tokenizer;

    private boolean initialize = false;

    public DelimiterIdentifyingTokenizer(int minTokens) {
        this.minTokens = minTokens;
    }

    public String[] tokens(String input) throws ImporterException {
        if (!initialize)
            identifyTokenizer(input);
        return tokenizer.tokens(input);
    }

    private void identifyTokenizer(String input) throws ImporterException {
        Tokenizer commaTokenizer = new CommaDelimitedTokenizer();
        String[] tokens = commaTokenizer.tokens(input);
        if (tokens.length >= minTokens) {
            tokenizer = commaTokenizer;
            return;
        }

        Tokenizer semiColonTokenizer = new SemiColonDelimitedTokenizer();
        tokens = semiColonTokenizer.tokens(input);
        if (tokens.length >= minTokens) {
            tokenizer = semiColonTokenizer;
            return;
        }

        Tokenizer whiteSpaceDelimiter = new WhitespaceDelimitedTokenizer();
        tokens = whiteSpaceDelimiter.tokens(input);
        if (tokens.length >= minTokens) {
            tokenizer = whiteSpaceDelimiter;
            return;
        }

        throw new ImporterException("Could not identify the delimiter");

    }
}
