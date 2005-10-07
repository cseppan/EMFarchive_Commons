package gov.epa.emissions.commons.io.importer.temporal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PacketReader {

    private BufferedReader fileReader;

    public PacketReader(File file) throws FileNotFoundException {
        fileReader = new BufferedReader(new FileReader(file));
    }

    public String identify() throws IOException {
        String header = fileReader.readLine().trim();
        return header.replaceAll("/", "");
    }

    public Record read() throws IOException {
        Record record = new Record();
        String line = fileReader.readLine();

        record.add(line.substring(0, 5));// code
        List tokens = parse(line.substring(5, (line.length() - 5)), 4);//months
        record.add(tokens);
        String last = line.substring(line.length() - 5, line.length());
        record.add(last);//total weights
        
        return record;
    }

    private List parse(String line, int size) {
        List list = new ArrayList();
        for (int i = 0; i < (line.length()/size); i++) {
            int current = i*size;
            list.add(line.substring(current, current + size));
        }
        
        return list;
    }

    public void close() throws IOException {
        fileReader.close();
    }

}
