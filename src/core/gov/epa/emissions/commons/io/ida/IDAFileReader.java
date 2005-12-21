package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IDAFileReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    private FileFormat fileFormat;

    private int lineNumber;

    private String line;

    public IDAFileReader(String source, FileFormat fileFormat) throws ImporterException {
        try {
            fileReader = new BufferedReader(new FileReader(source));
        } catch (FileNotFoundException e) {
            throw new ImporterException("File not found",e);
        }
        this.fileFormat = fileFormat;
        this.comments = new ArrayList();
        lineNumber = 0;
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws IOException {
        String line = fileReader.readLine();
        while (line != null) {
            lineNumber++;
            this.line = line;
            if (isData(line))
                return doRead(line);
            if (isComment(line))
                comments.add(line);

            line = fileReader.readLine();
        }
        return new TerminatorRecord();
    }

    private boolean isData(String line) {
        return !(line.trim().length() == 0) && (!isComment(line));
    }

    private Record doRead(String line) {
        Record record = new Record();
        String[] tokens = tokenizer(line);
        record.add(Arrays.asList(tokens));

        return record;
    }

    private String[] tokenizer(String line) {
        Column[] cols = fileFormat.cols();
        int startIndex = 0;
        String[] tokens = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            tokens[i] = line.substring(startIndex, startIndex + cols[i].width());
            startIndex = startIndex + cols[i].width();
        }
        return tokens;
    }

    private boolean isComment(String line) {
        return line.startsWith("#");
    }

    public List comments() {
        return comments;
    }

    public int lineNumber() {
        return lineNumber;
    }

    public String line() {
        return line;
    }

}
