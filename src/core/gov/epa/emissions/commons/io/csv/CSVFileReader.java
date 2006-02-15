package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.importer.CommaDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PipeDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.SemiColonDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;
import gov.epa.emissions.commons.io.importer.Tokenizer;
import gov.epa.emissions.commons.io.importer.WhitespaceDelimitedTokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CSVFileReader implements Reader {

    private static Log log = LogFactory.getLog(CSVFileReader.class);

    private BufferedReader fileReader;

    private List comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String line;

    private String[] cols;

    private List header;

    private File file;

    private String[] existedCols = { "Record_Id", "Dataset_Id", "Version", "Delete_Versions", "Comments" };

    public CSVFileReader(File file) throws ImporterException {
        try {
            fileReader = new BufferedReader(new FileReader(file));
            comments = new ArrayList();
            this.file = file;
            this.lineNumber = 0;
            detectDelimiter();
        } catch (FileNotFoundException e) {
            log.error("Importer failure: File not found" + "\n" + e);
            throw new ImporterException("Importer failure: File not found");
        } 
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws ImporterException {
        try {
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

        } catch (IOException e) {
            log.error("Importer failure: Error reading file" + "\n" + e);
            throw new ImporterException("Importer failure: Error reading file");
        }
        return new TerminatorRecord();
    }

    private boolean isData(String line) {
        return !(line.trim().length() == 0) && (!isComment(line));
    }

    private Record doRead(String line) throws ImporterException {
        Record record = new Record();
        String[] tokens = tokenizer.tokens(line);
        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].indexOf(":\\") >= 0) {
                tokens[i] = checkBackSlash(tokens[i]);
            }
        }
        record.add(Arrays.asList(tokens));

        return record;
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

    public List getHeader() {
        return header;
    }

    public String[] getCols() {
        return cols;
    }

    private void detectDelimiter() throws ImporterException {
        try {
            String line = fileReader.readLine();
            fileReader.mark((int) file.length()); // FIXME: what if file gets too big?

            if (getTokenizer(line))
                return;

            fileReader.reset();
            tokenizer = new WhitespaceDelimitedTokenizer();
            line = fileReader.readLine();
            while (isComment(line))
                line = fileReader.readLine();
            cols = underScoreTheSpace(tokenizer.tokens(line));
        } catch (IOException e) {
            log.error("Importer failure: Error reading file" + "\n" + e);
            throw new ImporterException("Importer failure: Error reading file");
        }
    }

    private boolean getTokenizer(String line) throws ImporterException {
        Pattern bar = Pattern.compile("[|]");
        try {
            for (; line != null; line = fileReader.readLine()) {
                if (!isComment(line) && line.split(",").length >= 2)
                    tokenizer = new CommaDelimitedTokenizer();
                else if (!isComment(line) && line.split(";").length >= 2)
                    tokenizer = new SemiColonDelimitedTokenizer();
                else if (!isComment(line) && bar.split(line).length >= 2)
                    tokenizer = new PipeDelimitedTokenizer();
                else
                    header.add(line);

                if (tokenizer != null) {
                    cols = underScoreTheSpace(tokenizer.tokens(line));
                    return true;
                }
            }
        } catch (IOException e) {
            log.error("Importer failure: Error reading file" + "\n" + e);
            throw new ImporterException("Importer failure: Error reading file");
        }

        return false;
    }

    private String[] underScoreTheSpace(String[] cols) {
        for (int i = 0; i < cols.length; i++) {
            cols[i] = cols[i].replace(' ', '_');
            cols[i] = checkExistCols(cols[i]);
        }

        return cols;
    }

    private String checkExistCols(String col) {
        for (int i = 0; i < existedCols.length; i++)
            if (col.equalsIgnoreCase(existedCols[i]))
                col += "_XXX";
        return col;
    }

    private String checkBackSlash(String col) {
        return col.replaceAll("\\\\", "\\\\\\\\");
    }

}
