package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;
import gov.epa.emissions.commons.io.importer.Tokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SpeciationCrossReferenceReader implements Reader {

    // private FileFormat fileFormat;

    private BufferedReader fileReader;

    private List comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String line;

    public SpeciationCrossReferenceReader(File file, FileFormat format, Tokenizer tokenizer)
            throws FileNotFoundException {
        // fileFormat = format;
        fileReader = new BufferedReader(new FileReader(file));
        comments = new ArrayList();
        this.tokenizer = tokenizer;
        this.lineNumber = 0;
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws IOException, ImporterException {
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

    private Record doRead(String line) throws ImporterException {
        Record record = new Record();
        String[] tokens = tokenizer.tokens(line);
        trimInlineComment(tokens);
        record.add(Arrays.asList(tokens));

        return record;
    }

    private void trimInlineComment(String[] tokens) {
        int length = tokens.length;
        if (length > 0) {
            String lastToken = tokens[length - 1].trim();
            if (lastToken.startsWith("!") && lastToken.length() > 128) {
                tokens[length - 1] = lastToken.substring(0, 128);
            }
        }
    }

    private boolean isComment(String line) {
        return (line.startsWith("#")) || (line.startsWith("/POINT DEFN/"));
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
