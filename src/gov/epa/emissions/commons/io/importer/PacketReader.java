package gov.epa.emissions.commons.io.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PacketReader implements Reader {

    private BufferedReader fileReader;

    private String identifier;

    private ColumnsMetadata cols;

    private List comments;

    // FIXME: get rid of this constructor in lieu of the other
    public PacketReader(File file, ColumnsMetadata cols) throws IOException {
        fileReader = new BufferedReader(new FileReader(file));

        String header = fileReader.readLine().trim();
        identifier = header.replaceAll("/", "");
        this.cols = cols;
        comments = new ArrayList();
    }

    public PacketReader(BufferedReader reader, String header, ColumnsMetadata cols) {
        fileReader = reader;
        identifier = header.replaceAll("/", "");
        this.cols = cols;
    }

    public String identify() {
        return identifier;
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
