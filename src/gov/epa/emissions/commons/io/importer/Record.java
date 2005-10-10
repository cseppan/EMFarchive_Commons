package gov.epa.emissions.commons.io.importer;

import java.util.ArrayList;
import java.util.List;

public class Record {

    private List tokens;

    public Record() {
        this.tokens = new ArrayList();
    }

    public String token(int position) {
        return (String) tokens.get(position);
    }

    public int size() {
        return tokens.size();
    }

    public void add(String token) {
        tokens.add(token);
    }

    public void add(List list) {
        tokens.addAll(list);
    }

    public boolean isEnd() {
        return false;
    }

}
