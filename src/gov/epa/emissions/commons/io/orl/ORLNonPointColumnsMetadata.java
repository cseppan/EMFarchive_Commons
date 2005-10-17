package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class ORLNonPointColumnsMetadata implements ORLColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public ORLNonPointColumnsMetadata(SqlDataTypes types) {
        String intType = types.intType();
        colTypes = new String[] { intType, types.stringType(10), types.stringType(4), types.stringType(6),
                types.stringType(2), types.stringType(6), types.stringType(16), types.realType(), types.realType(),
                types.realType(), types.realType(), types.realType() };

        colNames = new String[] { "FIPS", "SCC", "SIC", "MACT", "SRCTYPE", "NAICS", "POLL", "ANN_EMIS", "AVD_EMIS",
                "CEFF", "REFF", "RPEN" };
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
        return "ORL NonPoint";
    }

    public Column[] cols() {
        Column fips = new Column(new IntegerFormatter(), "FIPS");
        Column scc = new Column(new StringFormatter(10), "SCC");
        Column sic = new Column(new StringFormatter(4), "SIC");
        Column mact = new Column(new StringFormatter(6), "MACT");
        Column srcType = new Column(new StringFormatter(2), "SRCTYPE");
        Column naics = new Column(new StringFormatter(6), "NAICS");
        Column pollutant = new Column(new StringFormatter(16), "POLL");
        Column annualEmissions = new Column(new RealFormatter(), "ANN_EMIS");
        Column averageDailyEmissions = new Column(new RealFormatter(), "AVD_EMIS");
        Column ceff = new Column(new RealFormatter(), "CEFF");
        Column reff = new Column(new RealFormatter(), "REFF");
        Column rpen = new Column(new RealFormatter(), "RPEN");

        return new Column[] { fips, scc, sic, mact, srcType, naics, pollutant, annualEmissions, averageDailyEmissions,
                ceff, reff, rpen };
    }

}
