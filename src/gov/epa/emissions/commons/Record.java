package gov.epa.emissions.commons;

import java.util.ArrayList;
import java.util.Arrays;
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

    public List tokens() {
        return tokens;
    }

    public String toString() {
        return tokens.toString();
    }

    public String[] getTokens() {
        return (String[]) tokens.toArray(new String[0]);
    }

    public void setTokens(String[] tokensList) {
        tokens.clear();
        tokens.addAll(Arrays.asList(tokensList));
    }

}
