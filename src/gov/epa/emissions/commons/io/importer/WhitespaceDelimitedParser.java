package gov.epa.emissions.commons.io.importer;

import java.util.Arrays;

public class WhitespaceDelimitedParser implements Parser {

    private DelimitedInputTokenizer delimitedInputTokenizer;

    public WhitespaceDelimitedParser() {
        delimitedInputTokenizer = new DelimitedInputTokenizer();
    }

    public Record parse(String line) {
        Record record = new Record();
        String[] tokens = delimitedInputTokenizer.tokensWhitepaceDelimited(line);
        record.add(Arrays.asList(tokens));

        return record;
    }

}
