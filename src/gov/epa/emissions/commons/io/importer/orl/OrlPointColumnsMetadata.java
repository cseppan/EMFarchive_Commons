package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class OrlPointColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlPointColumnsMetadata(SqlDataTypes types) {
        String intType = types.getInt();
        colTypes = new String[] { intType, types.getString(15), types.getString(15), types.getString(15),
                types.getString(15), types.getString(40), types.getString(10), types.getString(2), types.getString(2),
                types.getReal(), types.getReal(), types.getReal(), types.getReal(), types.getReal(),
                types.getString(4), types.getString(6), types.getString(6), types.getString(1), types.getReal(),
                types.getReal(), types.smallInt(), types.getString(16), types.getReal(), types.getReal(),
                types.getReal(), types.getReal(), intType, intType };

        colNames = new String[] { "FIPS", "PLANTID", "POINTID", "STACKID", "SEGMENT", "PLANT", "SCC", "ERPTYPE",
                "SRCTYPE", "STKHGT", "STKDIAM", "STKTEMP", "STKFLOW", "STKVEL", "SIC", "MACT", "NAICS", "CTYPE",
                "XLOC", "YLOC", "UTMZ", "POLL", "ANN_EMIS", "AVD_EMIS", "CEFF", "REFF", "CPRI", "CSEC" };
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
