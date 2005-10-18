package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class ORLOnRoadColumnsMetadata implements ColumnsMetadata {

    private Column[] cols;

    public ORLOnRoadColumnsMetadata(SqlDataTypes types) {
        cols = createCols(types);
    }

    public String identify() {
        return "ORL OnRoad";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types) {
        Column fips = new Column("FIPS", types.intType(), new IntegerFormatter());
        Column scc = new Column("SCC", types.stringType(10), new StringFormatter(10));
        Column pollutant = new Column("POLL", types.stringType(16), new StringFormatter(16));
        Column annualEmissions = new Column("ANN_EMIS", types.realType(), new RealFormatter());
        Column averageDailyEmissions = new Column("AVD_EMIS", types.realType(), new RealFormatter());

        return new Column[] { fips, scc, pollutant, annualEmissions, averageDailyEmissions };
    }

}
