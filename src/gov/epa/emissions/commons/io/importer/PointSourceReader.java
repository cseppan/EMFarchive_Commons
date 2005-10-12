package gov.epa.emissions.commons.io.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PointSourceReader implements Reader {

    private DataReader delegate;

    private String header;

    public PointSourceReader(BufferedReader reader, ColumnsMetadata cols) throws IOException {
        header = parseHeader(reader.readLine());
        delegate = new DataReader(reader, new WhitespaceDelimitedParser());
    }

    public Record read() throws IOException {
        return delegate.read();
    }

    public List comments() {
        return delegate.comments();
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
}
