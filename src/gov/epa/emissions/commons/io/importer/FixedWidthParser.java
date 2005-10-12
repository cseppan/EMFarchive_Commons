package gov.epa.emissions.commons.io.importer;

public class FixedWidthParser implements Parser {

    private ColumnsMetadata cols;

    public FixedWidthParser(ColumnsMetadata cols) {
        this.cols = cols;
    }

    public Record parse(String line) {
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
}
