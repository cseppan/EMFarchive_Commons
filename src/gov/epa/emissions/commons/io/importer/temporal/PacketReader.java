package gov.epa.emissions.commons.io.importer.temporal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PacketReader {

    private BufferedReader fileReader;

    private String identifier;

    private ColumnsMetadata cols;

    public PacketReader(File file, ColumnsMetadata cols) throws IOException {
        fileReader = new BufferedReader(new FileReader(file));

        String header = fileReader.readLine().trim();
        identifier = header.replaceAll("/", "");
        this.cols = cols;
    }

    public String identify() {
        return identifier;
    }

    public Record read() throws IOException {
        String line = fileReader.readLine();
        if (isEnd(line))
            return new PacketTerminator();

        return doRead(line);
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

    public void close() throws IOException {
        fileReader.close();
    }

    public List allRecords() throws IOException {
        List records = new ArrayList();

        String line = fileReader.readLine();
        while (line != null && !isEnd(line)) {
            records.add(doRead(line));
            line = fileReader.readLine();
        }

        return records;
    }

    private boolean isEnd(String line) {
        return line.trim().equals("/END/");
    }

}
