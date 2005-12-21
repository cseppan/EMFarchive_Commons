package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.importer.FileFormat;
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
    
    private FileFormat fileFormat;
    
    private BufferedReader fileReader;

    private List comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String line;
    
    public SpeciationCrossReferenceReader(File file, FileFormat format, Tokenizer tokenizer) throws FileNotFoundException {
        fileFormat = format;
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
        
        int numCols = fileFormat.cols().length;
        int numTokens = tokens.length;
        String lastToken = tokens[numTokens - 1].trim();
        String[] newTokens = new String[numCols + 1];    
        
        for(int i = 0; i < numCols; i++){
            if(i < numTokens)
                newTokens[i] = tokens[i].trim();
            else
                newTokens[i] = "";
        }
        
        if(lastToken.startsWith("!")){
            newTokens[numTokens - 1] = "";
            newTokens[numCols] = lastToken;
        }else{
            newTokens[numCols] = "";
        }
        
        record.add(Arrays.asList(newTokens));

        return record;
    }

    private boolean isComment(String line) {
        return (line.startsWith("#")) || (line.startsWith("/POINT DEFN/"));
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

}
