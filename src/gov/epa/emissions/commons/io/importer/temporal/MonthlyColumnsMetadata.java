package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class MonthlyColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private String[] colTypes;

    private String[] colNames;

    public MonthlyColumnsMetadata(SqlDataTypes sqlDataType) {
        widths = new int[] { 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5 };

        String intType = sqlDataType.intType();
        colTypes = new String[] { intType, intType, intType, intType, intType, intType, intType, intType, intType,
                intType, intType, intType, intType, intType };

        colNames = new String[] { "Code", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                "Dec", "Total_Weights" };
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
