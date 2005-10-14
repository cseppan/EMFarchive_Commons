package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class WeeklyColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private String[] colTypes;

    private String[] colNames;

    public WeeklyColumnsMetadata(SqlDataTypes typeMapper) {
        widths = new int[] { 5, 4, 4, 4, 4, 4, 4, 4, 5 };

        String intType = typeMapper.intType();
        colTypes = new String[] { intType, intType, intType, intType, intType, intType, intType, intType, intType };

        colNames = new String[] { "Code", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "Total_Weights" };
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

    public String identify() {
        return "Weekly - Temporal Profile";
    }

}
