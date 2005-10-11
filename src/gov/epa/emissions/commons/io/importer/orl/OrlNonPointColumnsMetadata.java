package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class OrlNonPointColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlNonPointColumnsMetadata(SqlDataTypes types) {
        String intType = types.getInt();
        colTypes = new String[] { intType, types.getString(10), types.getString(4), types.getString(6),
                types.getString(2), types.getString(6), types.getString(16), types.getReal(), types.getReal(),
                types.getReal(), types.getReal(), types.getReal() };

        colNames = new String[] { "FIPS", "SCC", "SIC", "MACT", "SRCTYPE", "NAICS", "POLL", "ANN_EMIS", "AVD_EMIS",
                "CEFF", "REFF", "RPEN" };
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
