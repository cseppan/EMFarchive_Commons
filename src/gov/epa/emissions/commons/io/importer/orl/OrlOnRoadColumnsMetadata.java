package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.exporter.orl.ORLColumnsMetadata;

public class OrlOnRoadColumnsMetadata implements ORLColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlOnRoadColumnsMetadata(SqlDataTypes types) {
        colTypes = new String[] { types.intType(), types.stringType(10), types.stringType(16), types.realType(),
                types.realType() };

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

    public String identify() {
        return "ORL OnRoad";
    }

    public Column[] cols() {
        Column fips = new Column();

        return new Column[] { fips };
    }

}
