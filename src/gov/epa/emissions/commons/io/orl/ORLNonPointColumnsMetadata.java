package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class ORLNonPointColumnsMetadata implements ColumnsMetadata {

    private SqlDataTypes types;

    public ORLNonPointColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "ORL NonPoint";
    }

    public Column[] cols() {
        Column fips = new Column("FIPS", types.intType(), new IntegerFormatter());
        Column scc = new Column("SCC", types.stringType(10), new StringFormatter(10));
        Column sic = new Column("SIC", types.stringType(4), new StringFormatter(4));
        Column mact = new Column("MACT", types.stringType(6), new StringFormatter(6));
        Column srcType = new Column("SRCTYPE", types.stringType(2), new StringFormatter(2));
        Column naics = new Column("NAICS", types.stringType(6), new StringFormatter(6));
        Column pollutant = new Column("POLL", types.stringType(16), new StringFormatter(16));
        Column annualEmissions = new Column("ANN_EMIS", types.realType(), new RealFormatter());
        Column averageDailyEmissions = new Column("AVD_EMIS", types.realType(), new RealFormatter());
        Column ceff = new Column("CEFF", types.realType(), new RealFormatter());
        Column reff = new Column("REFF", types.realType(), new RealFormatter());
        Column rpen = new Column("RPEN", types.realType(), new RealFormatter());

        return new Column[] { fips, scc, sic, mact, srcType, naics, pollutant, annualEmissions, averageDailyEmissions,
                ceff, reff, rpen };
    }

}
