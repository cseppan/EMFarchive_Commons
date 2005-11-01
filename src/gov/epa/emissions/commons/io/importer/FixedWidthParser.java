package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Column;

public class FixedWidthParser implements Parser {

    private FileFormat fileFormat;

    public FixedWidthParser(FileFormat fileFormat) {
        this.fileFormat = fileFormat;
    }

    public Record parse(String line) {
        Record record = new Record();
        addTokens(line, record, fileFormat.cols());
        return record;
    }

    private void addTokens(String line, Record record, Column[] columns) {
        int offset = 0;
        for (int i = 0; i < columns.length; i++) {
            record.add(line.substring(offset, offset + columns[i].width()));
            offset += columns[i].width();
        }
    }
}
