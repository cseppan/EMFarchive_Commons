package gov.epa.emissions.commons.io.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DelimitedFileReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    public DelimitedFileReader(File file) throws FileNotFoundException {
        fileReader = new BufferedReader(new FileReader(file));
        comments = new ArrayList();
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws IOException {
        String line = fileReader.readLine();

        while (line != null) {
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

    private Record doRead(String line) {
        Record record = new Record();
        String[] tokens = new DelimitedInputTokenizer().tokensUsingSpace(line);
        record.add(Arrays.asList(tokens));

        return record;
    }

    private boolean isComment(String line) {
        return line.startsWith("#");
    }

    public List comments() {
        return comments;
    }

}
