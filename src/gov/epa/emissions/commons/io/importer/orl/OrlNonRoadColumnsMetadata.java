package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class OrlNonRoadColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlNonRoadColumnsMetadata(SqlDataTypes types) {
        colTypes = new String[] { types.intType(), types.stringType(10), types.stringType(16), types.realType(), types.realType(),
                types.realType(), types.realType(), types.realType() };

        colNames = new String[] { "FIPS", "SCC", "POLL", "ANN_EMIS", "AVD_EMIS", "CEFF", "REFF", "RPEN" };
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
        return "ORL NonRoad";
    }

}
