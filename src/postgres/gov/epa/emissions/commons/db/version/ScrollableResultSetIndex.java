package gov.epa.emissions.commons.db.version;

public class ScrollableResultSetIndex {
    private int start = 0;

    static final int FETCH_SIZE = 10000;

    public int start() {
        return start;
    }

    public int newStart(int index) {
        int steps = (index / FETCH_SIZE);
        start = (steps * FETCH_SIZE);
        return start;
    }

    public int end() {
        return (start + FETCH_SIZE);
    }

    public boolean inRange(int index) {
        return (start <= index) && (index < end());
    }

    public int relative(int index) {
        return index % FETCH_SIZE;
    }

}
