package gov.epa.emissions.commons.io.importer.temporal;

import java.util.List;

public class PacketTerminator extends Record {

    public void add(List list) {
        // No Op
    }

    public void add(String token) {
        // No Op
    }

    public int size() {
        return 0;
    }

    public String token(int position) {
        return null;
    }

}
