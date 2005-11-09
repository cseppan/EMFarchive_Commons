package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class DelimiterIdentifyingFileReader implements Reader {

    private DelimitedFileReader reader;

    public DelimiterIdentifyingFileReader(File file, int minTokens) throws FileNotFoundException {
        reader = new DelimitedFileReader(file, new DelimiterIdentifyingTokenizer(minTokens));
    }

    public void close() throws IOException {
        reader.close();
    }

    public Record read() throws IOException {
        return reader.read();
    }

    public List comments() {
        return reader.comments();
    }
    
    public int lineNumber(){
        return reader.lineNumber();
    }

}
