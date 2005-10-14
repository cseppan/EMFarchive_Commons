package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class OrlPointColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlPointColumnsMetadata(SqlDataTypes types) {
        String intType = types.intType();
        colTypes = new String[] { intType, types.stringType(15), types.stringType(15), types.stringType(15),
                types.stringType(15), types.stringType(40), types.stringType(10), types.stringType(2), types.stringType(2),
                types.realType(), types.realType(), types.realType(), types.realType(), types.realType(),
                types.stringType(4), types.stringType(6), types.stringType(6), types.stringType(1), types.realType(),
                types.realType(), types.smallInt(), types.stringType(16), types.realType(), types.realType(),
                types.realType(), types.realType(), intType, intType };

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

    public String identify() {
        return "ORL Point";
    }

}
