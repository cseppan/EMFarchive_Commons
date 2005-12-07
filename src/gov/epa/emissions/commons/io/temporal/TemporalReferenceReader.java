package gov.epa.emissions.commons.io.temporal;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.importer.DataReader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.WhitespaceDelimitedParser;

public class TemporalReferenceReader implements Reader {
    private DataReader delegate;

    private String header;

    public TemporalReferenceReader(BufferedReader reader) throws IOException {
        header = readHeader(reader);
        delegate = new DataReader(reader, new WhitespaceDelimitedParser());
    }

    public Record read() throws IOException {
        return delegate.read();
    }

    public List comments() {
        return delegate.comments();
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
            return header.substring(m.start() + 1, m.end() - 1);
        }

        return null;
    }

    public String identify() {
        return header;
    }

    public void close() {
        delegate.close();
    }

    public int lineNumber() {
        // TODO Auto-generated method stub
        return 0;
    }

    public String line() {
        // TODO Auto-generated method stub
        return null;
    }
}
