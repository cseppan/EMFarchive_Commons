package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LineReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    public LineReader(File file) throws FileNotFoundException {
        fileReader = new BufferedReader(new FileReader(file));
        comments = new ArrayList();
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws IOException {
        String line = fileReader.readLine();

        if (line != null) {
            Record record = new Record();
            record.add(line);
            return record;
        }
        return new TerminatorRecord();
    }

    public List comments() {
        return comments;
    }

    public int lineNumber() {
        // TODO Auto-generated method stub
        return 0;
    }
}
