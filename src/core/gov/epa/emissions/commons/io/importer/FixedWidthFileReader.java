package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.CustomCharSetInputStreamReader;
import gov.epa.emissions.commons.io.FileFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class FixedWidthFileReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    private int lineNumber;

    private String line;

    private FixedWidthParser fixedWidthParser;

    public FixedWidthFileReader(String source, FileFormat fileFormat) throws ImporterException {
        try {
            fileReader = new BufferedReader(new CustomCharSetInputStreamReader(new FileInputStream(source)));
        } catch (FileNotFoundException e) {
            throw new ImporterException("File not found", e);
        } catch (UnsupportedEncodingException e) {
            throw new ImporterException("Encoding char set not supported.");
        }
        this.fixedWidthParser = new FixedWidthParser(fileFormat);
        this.comments = new ArrayList();
        lineNumber = 0;
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws IOException {
        String line = fileReader.readLine();
        //System.out.println("Line read through IDA file reader: " + line); //debug purpose
        while (line != null) {
            lineNumber++;
            this.line = line;
            if (isData(line))
                return fixedWidthParser.parse(line);
            if (isComment(line))
                comments.add(line);

            line = fileReader.readLine();
        }
        return new TerminatorRecord();
    }

    private boolean isData(String line) {
        return !(line.trim().length() == 0) && (!isComment(line));
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
