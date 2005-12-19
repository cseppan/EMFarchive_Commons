package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class DelimitedFileReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String line;

    public DelimitedFileReader(File file, Tokenizer tokenizer) throws FileNotFoundException {
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
    
    //Added to remove header lines
    public String[] readHeader(int numLines) throws IOException {
        List header = new ArrayList();
        for(int i = 0; i < numLines; i++) {
            header.add(fileReader.readLine());
        }
        
        return (String[])header.toArray(new String[0]);
    }
    
    //Added to remove header lines
    public String[] readHeader(String regex) throws IOException {
        List header = new ArrayList();
        String line = fileReader.readLine();
        
        Pattern pattern = Pattern.compile(regex);
        if (pattern.split(line).length < 3) {
            header.add(line);
            line = fileReader.readLine();
        }
        
        header.add(line); //Table header - Column Names
        header.add(line); //Table header - Units
        header.add(line); //FIXME: Assume one more line of table border
        
        return (String[])header.toArray(new String[0]);
    }

}
