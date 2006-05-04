package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.importer.DataReader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.WhitespaceDelimitedParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemporalReferenceReader implements Reader {
    private DataReader delegate;

    private String header;

    public TemporalReferenceReader(BufferedReader reader, int lineNumber) throws IOException {
        this.header = readHeader(reader);
        this.delegate = new DataReader(reader, lineNumber, new WhitespaceDelimitedParser());
    }

    public Record read() throws IOException {
        return delegate.read();
    }

    public List comments() {
        List comments = delegate.comments();
        comments.add(header);
        
        return comments;
    }
    
    private String readHeader(BufferedReader reader) throws IOException {
        reader.mark(255); // data file has less than 255 characters in a line
        String head = parseHeader(reader.readLine());
        if(head == null)
            reader.reset();
        
        return head;
    }

    private String parseHeader(String header) {
        Pattern p = Pattern.compile("/[a-zA-Z\\s]+/");
        Matcher m = p.matcher(header);
        if (m.find()) {
            return header;
        }

        return null;
    }
    
    public String identify() {
        if(header != null)
            return "Point Temporal Reference";
        
        return "Area/Mobile Temporal Reference";
    }

    public void close() throws IOException {
        delegate.close();
    }

    public int lineNumber() {
        return delegate.lineNumber();
    }

    public String line() {
        return delegate.line();
    }
}
