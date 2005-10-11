package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class OrlOnRoadColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlOnRoadColumnsMetadata(SqlDataTypes types) {
        colTypes = new String[] { types.getInt(), types.getString(10), types.getString(16), types.getReal(),
                types.getReal() };

        colNames = new String[] { "FIPS", "SCC", "POLL", "ANN_EMIS", "AVD_EMIS" };
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
