package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

import java.util.ArrayList;
import java.util.List;

public class ORLOnRoadColumnsMetadata implements ColumnsMetadata {

    private Column[] cols;

    public ORLOnRoadColumnsMetadata(SqlDataTypes types) {
        cols = createCols(types);
    }

    public int[] widths() {
        return null;
    }

    public String[] colTypes() {
        Column[] cols = cols();

        List sqlTypes = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            sqlTypes.add(cols[i].sqlType());
        }

        return (String[]) sqlTypes.toArray(new String[0]);
    }

    // FIXME: duplicate code in each ORLColumnMetadata
    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String identify() {
        return "ORL OnRoad";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types) {
        Column fips = new Column(types.intType(), new IntegerFormatter(), "FIPS");
        Column scc = new Column(types.stringType(10), new StringFormatter(10), "SCC");
        Column pollutant = new Column(types.stringType(16), new StringFormatter(16), "POLL");
        Column annualEmissions = new Column(types.realType(), new RealFormatter(), "ANN_EMIS");
        Column averageDailyEmissions = new Column(types.realType(), new RealFormatter(), "AVD_EMIS");

        return new Column[] { fips, scc, pollutant, annualEmissions, averageDailyEmissions };
    }

}
