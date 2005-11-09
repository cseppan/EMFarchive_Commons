package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class WhitespaceDelimitedFileReader implements Reader {

    private DelimitedFileReader reader;

    public WhitespaceDelimitedFileReader(File file) throws FileNotFoundException {
        reader = new DelimitedFileReader(file, new WhitespaceDelimitedTokenizer());
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

    public int lineNumber() {
        // TODO Auto-generated method stub
        return 0;
    }

}
