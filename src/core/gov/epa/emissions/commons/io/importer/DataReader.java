package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    private Parser parser;
    
    private int lineNumber;

    private String line;

    public DataReader(BufferedReader reader, int lineNumber, Parser parser) {
        fileReader = reader;
        this.lineNumber = lineNumber;
        this.parser = parser;
        comments = new ArrayList();
    }

    public Record read() throws IOException {
        for (String line = fileReader.readLine(); !isEnd(line); line = fileReader.readLine()) {
            this.line = line;
            this.lineNumber++;
            
            if (isExportInfo(line)) 
                continue;
            if (isData(line))
                return parser.parse(line);
            if (isComment(line))
                comments.add(line);
        }

        return new TerminatorRecord();
    }

    private boolean isExportInfo(String line) {
        return line == null ? false : line.trim().startsWith("#EXPORT_");
    }
    
    private boolean isEnd(String line) {
        return line == null;
    }

    private boolean isData(String line) {
        return !(line.trim().length() == 0) && (!isComment(line));
    }

    private boolean isComment(String line) {
        return line.trim().startsWith("#");
    }

    public List comments() {
        return comments;
    }

    public void close() throws IOException {
       fileReader.close();
    }
    
    public int lineNumber() {
        return lineNumber;
    }

    public String line() {
        return line;
    }

}
