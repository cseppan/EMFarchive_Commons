package gov.epa.emissions.commons.io.importer;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public class FixedWidthPacketReader implements PacketReader {

    private PacketReaderImpl delegate;

    public FixedWidthPacketReader(BufferedReader reader, String headerLine, ColumnsMetadata cols) {
        delegate = new PacketReaderImpl(reader, headerLine, new FixedWidthParser(cols));
    }

    public Record read() throws IOException {
        return delegate.read();
    }

    public List comments() {
        return delegate.comments();
    }

    public String identify() {
        return delegate.identify();
    }
}
