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

    public PacketReader(File file) throws IOException {
        fileReader = new BufferedReader(new FileReader(file));

        String header = fileReader.readLine().trim();
        identifier = header.replaceAll("/", "");
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

        // TODO: Monthly-specific
        record.add(line.substring(0, 5));// code
        List tokens = parse(line.substring(5, (line.length() - 5)), 4);// months
        record.add(tokens);
        String last = line.substring(line.length() - 5, line.length());
        record.add(last);// total weights

        return record;
    }

    private List parse(String line, int size) {
        List list = new ArrayList();
        for (int i = 0; i < (line.length() / size); i++) {
            int current = i * size;
            list.add(line.substring(current, current + size));
        }

        return list;
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
