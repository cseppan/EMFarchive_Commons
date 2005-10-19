package gov.epa.emissions.commons.io.importer;

public class DelimiterIdentifyingTokenizer implements Tokenizer {

    private int minTokens;

    private Tokenizer commaTokenizer;

    private SemiColonDelimitedTokenizer semiColonTokenizer;

    public DelimiterIdentifyingTokenizer(int minTokens) {
        this.minTokens = minTokens;
        commaTokenizer = new CommaDelimitedTokenizer();
        semiColonTokenizer = new SemiColonDelimitedTokenizer();
    }

    public String[] tokens(String input) {
        String[] commaDelimited = commaTokenizer.tokens(input);
        if (commaDelimited.length >= minTokens)
            return commaDelimited;

        String[] semiColonDelimited = semiColonTokenizer.tokens(input);
        if (semiColonDelimited.length >= minTokens)
            return semiColonDelimited;

        return null;
    }
}
