package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class OrlNonPointColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlNonPointColumnsMetadata(SqlDataTypes types) {
        String intType = types.intType();
        colTypes = new String[] { intType, types.stringType(10), types.stringType(4), types.stringType(6),
                types.stringType(2), types.stringType(6), types.stringType(16), types.realType(), types.realType(),
                types.realType(), types.realType(), types.realType() };

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

    public String identify() {
        return "ORL NonPoint";
    }

}
