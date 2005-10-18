package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class ORLNonRoadColumnsMetadata implements ColumnsMetadata {

    private SqlDataTypes types;

    public ORLNonRoadColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public int[] widths() {
        return null;
    }

    public String identify() {
        return "ORL NonRoad";
    }

    public Column[] cols() {
        Column fips = new Column(types.intType(), new IntegerFormatter(), "FIPS");
        Column scc = new Column(types.stringType(10), new StringFormatter(10), "SCC");
        Column pollutant = new Column(types.stringType(16), new StringFormatter(16), "POLL");
        Column annualEmissions = new Column(types.realType(), new RealFormatter(), "ANN_EMIS");
        Column averageDailyEmissions = new Column(types.realType(), new RealFormatter(), "AVD_EMIS");
        Column ceff = new Column(types.realType(), new RealFormatter(), "CEFF");
        Column reff = new Column(types.realType(), new RealFormatter(), "REFF");
        Column rpen = new Column(types.realType(), new RealFormatter(), "RPEN");

        return new Column[] { fips, scc, pollutant, annualEmissions, averageDailyEmissions, ceff, reff, rpen };
    }

}
