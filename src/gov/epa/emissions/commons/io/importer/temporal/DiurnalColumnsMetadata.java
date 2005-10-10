package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class DiurnalColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private String[] colTypes;

    private String[] colNames;

    public DiurnalColumnsMetadata(SqlDataTypes typeMapper) {
        widths = new int[] { 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5 };

        String intType = typeMapper.getInt();
        colTypes = new String[] { intType, intType, intType, intType, intType, intType, intType, intType, intType,
                intType, intType, intType, intType, intType, intType, intType, intType, intType, intType, intType,
                intType, intType, intType, intType, intType, intType };

        colNames = new String[] { "Code", "hr0", "hr1", "hr2", "hr3", "hr4", "hr5", "hr6", "hr7", "hr8", "hr9", "hr10",
                "hr11", "hr12", "hr13", "hr14", "hr15", "hr16", "hr17", "hr18", "hr19", "hr20", "hr21", "hr22", "hr23",
                "Total_Weights" };
    }

    public int[] widths() {
        return widths;
    }

    public String[] colTypes() {
        return colTypes;
    }

    public String[] colNames() {
        return colNames;
    }

}
