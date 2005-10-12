package gov.epa.emissions.commons.io.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PacketReader implements Reader {

    private BufferedReader fileReader;

    private String header;

    private ColumnsMetadata cols;

    private List comments;

    public PacketReader(BufferedReader reader, String headerLine, ColumnsMetadata cols) {
        fileReader = reader;
        header = parseHeader(headerLine);
        this.cols = cols;
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
        String line = fileReader.readLine();

        while (!isEnd(line)) {
            if (isData(line))
                return doRead(line);
            if (isComment(line))
                comments.add(line);

            line = fileReader.readLine();
        }

        return new TerminatorRecord();
    }

    private Record doRead(String line) {
        Record record = new Record();
        addTokens(line, record, cols.widths());
        return record;
    }

    private void addTokens(String line, Record record, int[] widths) {
        int offset = 0;
        for (int i = 0; i < widths.length; i++) {
            record.add(line.substring(offset, offset + widths[i]));
            offset += widths[i];
        }
    }

    private boolean isEnd(String line) {
        return line.trim().equals("/END/");
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

}
