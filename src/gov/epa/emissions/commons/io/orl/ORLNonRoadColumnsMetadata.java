package gov.epa.emissions.commons.io.orl;

import java.util.ArrayList;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class ORLNonRoadColumnsMetadata implements ORLColumnsMetadata {

    private String[] colTypes;

    public ORLNonRoadColumnsMetadata(SqlDataTypes types) {
        colTypes = new String[] { types.intType(), types.stringType(10), types.stringType(16), types.realType(),
                types.realType(), types.realType(), types.realType(), types.realType() };
    }

    public int[] widths() {
        return null;
    }

    public String[] colTypes() {
        return colTypes;
    }

    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String identify() {
        return "ORL NonRoad";
    }

    public Column[] cols() {
        Column fips = new Column(new IntegerFormatter(), "FIPS");
        Column scc = new Column(new StringFormatter(10), "SCC");
        Column pollutant = new Column(new StringFormatter(16), "POLL");
        Column annualEmissions = new Column(new RealFormatter(), "ANN_EMIS");
        Column averageDailyEmissions = new Column(new RealFormatter(), "AVD_EMIS");
        Column ceff = new Column(new RealFormatter(), "CEFF");
        Column reff = new Column(new RealFormatter(), "REFF");
        Column rpen = new Column(new RealFormatter(), "RPEN");

        return new Column[] { fips, scc, pollutant, annualEmissions, averageDailyEmissions, ceff, reff, rpen };
    }

}
