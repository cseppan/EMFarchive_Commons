package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PacketReaderImpl implements PacketReader {

    private BufferedReader fileReader;

    private String header;

    private List comments;

    private Parser parser;

    public PacketReaderImpl(BufferedReader reader, String headerLine, Parser parser) {
        fileReader = reader;
        header = parseHeader(headerLine);
        this.parser = parser;
        comments = new ArrayList();
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

    public Record read() throws IOException {
        for (String line = fileReader.readLine(); !isEnd(line); line = fileReader.readLine()) {
            if (isData(line))
                return parser.parse(line);
            if (isComment(line))
                comments.add(line);
        }

        return new TerminatorRecord();
    }

    private boolean isEnd(String line) {
        return (line == null) || (line.trim().equals("/END/"));
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

    public void close() {
        // no op
    }

    public int lineNumber() {
        // TODO Auto-generated method stub
        return 0;
    }

}
