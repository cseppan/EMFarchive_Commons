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
        initialize = true;

        Tokenizer commaTokenizer = commaTokenizer(input);
        if (commaTokenizer != null) {
            tokenizer = commaTokenizer;
            return;
        }

        Tokenizer semiColonTokenizer = semiColonTokenizer(input);
        if (semiColonTokenizer != null) {
            tokenizer = semiColonTokenizer;
            return;
        }

        Tokenizer whiteSpaceTokenizer = whitespaceTokenizer(input);
        if (whiteSpaceTokenizer != null) {
            tokenizer = whiteSpaceTokenizer;
            return;
        }

        throw new ImporterException("Could not identify the delimiter");

    }

    private Tokenizer commaTokenizer(String input) throws ImporterException {
        Tokenizer commaTokenizer = new CommaDelimitedTokenizer();
        try {
            String[] tokens = commaTokenizer.tokens(input);
            if (tokens.length >= minTokens) {
                return commaTokenizer;
            }
            return null;
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private Tokenizer semiColonTokenizer(String input) throws ImporterException {
        Tokenizer semiColonTokenizer = new SemiColonDelimitedTokenizer();
        try {
            String[] tokens = semiColonTokenizer.tokens(input);
            if (tokens.length >= minTokens) {
                return semiColonTokenizer;
            }
            return null;
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private Tokenizer whitespaceTokenizer(String input) throws ImporterException {
        Tokenizer whiteSpaceTokenizer = new WhitespaceDelimitedTokenizer();
        try {
            String[] tokens = whiteSpaceTokenizer.tokens(input);
            if (tokens.length >= minTokens) {
                return whiteSpaceTokenizer;
            }
            return null;
        } catch (IllegalStateException e) {
            return null;
        }
    }
}
