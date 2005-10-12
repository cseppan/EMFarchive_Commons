package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;

public class DelimitedColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public DelimitedColumnsMetadata(int cols, SqlDataTypes typeMapper) {
        // TODO: is the size sufficient ?
        colTypes = new String[cols];
        colNames = new String[cols];

        for (int i = 0; i < cols; i++) {
            colTypes[i] = typeMapper.stringType(32);
            colNames[i] = "Col_" + i;
        }
    }

    public int[] widths() {
        return null;
    }

    public String[] colTypes() {
        return colTypes;
    }

    public String[] colNames() {
        return colNames;
    }

}
