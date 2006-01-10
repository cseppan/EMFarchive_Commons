package gov.epa.emissions.commons.io.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.importer.CommaDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PipeDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.SemiColonDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;
import gov.epa.emissions.commons.io.importer.Tokenizer;
import gov.epa.emissions.commons.io.importer.WhitespaceDelimitedTokenizer;

public class CSVFileReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String line;
    
    private String[] cols;
    
    private List header;
    
    private File file;
    
    public CSVFileReader(File file) throws FileNotFoundException,IOException,ImporterException {
        fileReader = new BufferedReader(new FileReader(file));
        comments = new ArrayList();
        this.file = file;
        this.lineNumber = 0;
        detectDelimiter();
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
        record.add(Arrays.asList(tokens));

        return record;
    }

    private boolean isComment(String line) {
        return line.startsWith("#");
    }

    public List comments() {
        return comments;
    }
    
    public int lineNumber(){
        return lineNumber;
    }

    public String line() {
        return line;
    }
    
    public List getHeader() {
        return header;
    }
    
    public String[] getCols(){
        return cols;
    }
 
    private void detectDelimiter() throws IOException,ImporterException {
        Pattern bar = Pattern.compile("[|]");
        String line = fileReader.readLine();
        fileReader.mark((int)file.length()); //FIXME: what if file gets too big?
        
        for(; line != null; line = fileReader.readLine()) {
            if(!isComment(line) && line.split(",").length >= 2) 
                tokenizer = new CommaDelimitedTokenizer();
            else if(!isComment(line) && line.split(";").length >= 2) 
                tokenizer = new SemiColonDelimitedTokenizer();
            else if (!isComment(line) && bar.split(line).length >= 2) 
                tokenizer = new PipeDelimitedTokenizer();
            else
                header.add(line);
            
            if(tokenizer != null) {
                cols = tokenizer.tokens(line);
                return ;
            }
        }
        
        fileReader.reset();
        tokenizer = new WhitespaceDelimitedTokenizer();
        line = fileReader.readLine();
        while(isComment(line))
            line = fileReader.readLine();
        cols = tokenizer.tokens(line);
        return ;
    }
    
}