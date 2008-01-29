package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.PostgreSQLKeyWords;
import gov.epa.emissions.commons.io.CustomCharSetInputStreamReader;
import gov.epa.emissions.commons.io.importer.CommaDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;
import gov.epa.emissions.commons.io.importer.Tokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CSVFileReader implements Reader {

    private static Log log = LogFactory.getLog(CSVFileReader.class);

    private BufferedReader fileReader;

    private List<String> comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String currentLine;

    private String[] cols;

    private List<String> header;

    private File file;

    private String[] existedCols = { "Record_Id", "Dataset_Id", "Version", "Delete_Versions", "Comments", "DESC" };

    public CSVFileReader(File file) throws ImporterException {
        try {
            fileReader = new BufferedReader(new CustomCharSetInputStreamReader(new FileInputStream(file)));
            comments = new ArrayList<String>();
            header = new ArrayList<String>();
            this.file = file;
            this.lineNumber = 0;
            detectDelimiter();
        } catch (FileNotFoundException e) {
            log.error("Importer failure: File not found" + "\n" + e);
            throw new ImporterException("Importer failure: File not found");
        } catch (UnsupportedEncodingException e) {
            log.error("Importer failure: character set encoding not supported" + "\n" + e);
            throw new ImporterException("Importer failure: character set encoding not supported");
        }
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws ImporterException {
        try {
            String line = null;

            while ((line = fileReader.readLine()) != null) {
                lineNumber++;
                this.currentLine = line.trim();
                
                if (isComment(currentLine)) {
                    if (isExportInfo(currentLine))
                        continue; // rip off the export info lines
                    
                    comments.add(currentLine);
                    continue;
                }

                if (currentLine.length() != 0)
                    return doRead(line);
            }

        } catch (IOException e) {
            log.error("Importer failure: Error reading file" + "\n" + e);
            throw new ImporterException("Importer failure: Error reading file");
        }
        return new TerminatorRecord();
    }

    private boolean isExportInfo(String line) {
        return line == null ? false : (line.trim().startsWith("#EXPORT_")); // || line.startsWith("#EMF_"));
    }

    private Record doRead(String line) throws ImporterException {
        Record record = new Record();
        String[] tokens = tokenizer.tokens(line);
        for (int i = 0; i < tokens.length; i++) {
            // if (tokens[i].indexOf(":\\") >= 0) {//add escape characters => insertable into postgres
            tokens[i] = checkBackSlash(tokens[i]);
            // }
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
        return currentLine;
    }

    public List getHeader() {
        return header;
    }

    public String[] getCols() {
        return cols;
    }

    private void detectDelimiter() throws ImporterException {
        if (file.length() == 0)
            throw new ImporterException("File: " + file.getAbsolutePath() + " is empty.");

        if (getTokenizer())
            return;
    }

    private boolean getTokenizer() throws ImporterException {
        try {
            String lineRead = fileReader.readLine();
            
            for (; lineRead != null; lineRead = fileReader.readLine()) {
                if (isExportInfo(lineRead))
                    continue;
                else if (isComment(lineRead))
                    header.add(lineRead);
                else if (lineRead.split(",").length >= 2)
                    tokenizer = new CommaDelimitedTokenizer();

                if (tokenizer != null) {
                    cols = underScoreTheSpace(tokenizer.tokens(lineRead));
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
            String temp = cols[i].replace(' ', '_');
            temp = (PostgreSQLKeyWords.reserved(temp.toUpperCase())) ? temp + "_" : temp;
            cols[i] = checkExistCols(temp);
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
