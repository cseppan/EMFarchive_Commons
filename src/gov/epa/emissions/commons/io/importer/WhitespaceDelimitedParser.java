package gov.epa.emissions.commons.io.importer;

import java.util.Arrays;

public class WhitespaceDelimitedParser implements Parser {

    private WhitespaceDelimitedTokenizer tokenizer;

    public WhitespaceDelimitedParser() {
        tokenizer = new WhitespaceDelimitedTokenizer();
    }

    public Record parse(String line) {
        Record record = new Record();
        String[] tokens = tokenizer.tokens(line);
        record.add(Arrays.asList(tokens));

        return record;
    }

}
