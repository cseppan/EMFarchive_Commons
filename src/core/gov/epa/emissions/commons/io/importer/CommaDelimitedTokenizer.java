package gov.epa.emissions.commons.io.importer;

public class CommaDelimitedTokenizer implements Tokenizer {

    private DelimitedTokenizer delegate;

    private String pattern;

    public CommaDelimitedTokenizer() {
        pattern = "[^,]+";
        delegate = new DelimitedTokenizer(pattern);
        
    }

    public String[] tokens(String input) {
        input = padding(input);
        return delegate.doTokenize(input);
    }

    private String padding(String input) {
        input = input.trim();
        if (input.startsWith(","))
            input = "," + input;
        if (input.endsWith(","))
            input = input + ",";
        input = input.replaceAll(",,", ", ,").replaceAll(",,",", ,");

        return input;
    }

}
