package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class PointSourceColumnsMetadata implements ColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public PointSourceColumnsMetadata(SqlDataTypes types) {
        colTypes = new String[] { types.stringType(10), types.intType(), types.intType(), types.intType(),
                types.charType() };

        colNames = new String[] { "SCC", "Monthly_Code", "Weekly_Code", "Diurnal_Code", "Pollutant" };
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
