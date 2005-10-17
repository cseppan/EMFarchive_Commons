package gov.epa.emissions.commons.io.orl;

import java.util.ArrayList;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.SmallIntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class ORLPointColumnsMetadata implements ORLColumnsMetadata {

    private SqlDataTypes types;

    public ORLPointColumnsMetadata(SqlDataTypes types) {
        this.types = types;
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

    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String identify() {
        return "ORL Point";
    }

    public Column[] cols() {
        Column fips = new Column(types.intType(), new IntegerFormatter(), "FIPS");
        Column plantId = new Column(types.stringType(15), new StringFormatter(15), "PLANTID");
        Column pointId = new Column(types.stringType(15), new StringFormatter(15), "POINTID");
        Column stackId = new Column(types.stringType(15), new StringFormatter(15), "STACKID");
        Column segment = new Column(types.stringType(15), new StringFormatter(15), "SEGMENT");
        Column plant = new Column(types.stringType(40), new StringFormatter(40), "PLANT");
        Column scc = new Column(types.stringType(10), new StringFormatter(10), "SCC");
        Column erpType = new Column(types.stringType(2), new StringFormatter(2), "ERPTYPE");
        Column srcType = new Column(types.stringType(2), new StringFormatter(2), "SRCTYPE");
        Column stkhgt = new Column(types.realType(), new RealFormatter(), "STKHGT");
        Column stkdiam = new Column(types.realType(), new RealFormatter(), "STKDIAM");
        Column stktemp = new Column(types.realType(), new RealFormatter(), "STKTEMP");
        Column stkflow = new Column(types.realType(), new RealFormatter(), "STKFLOW");
        Column stkvel = new Column(types.realType(), new RealFormatter(), "STKVEL");
        Column sic = new Column(types.stringType(4), new StringFormatter(4), "SIC");
        Column mact = new Column(types.stringType(6), new StringFormatter(6), "MACT");
        Column naics = new Column(types.stringType(6), new StringFormatter(6), "NAICS");
        Column ctype = new Column(types.stringType(1), new StringFormatter(1), "CTYPE");
        Column xloc = new Column(types.realType(), new RealFormatter(), "XLOC");
        Column yloc = new Column(types.realType(), new RealFormatter(), "YLOC");
        Column utmz = new Column(types.smallInt(), new SmallIntegerFormatter(), "UTMZ");
        Column pollutant = new Column(types.stringType(16), new StringFormatter(16), "POLL");
        Column annualEmissions = new Column(types.realType(), new RealFormatter(), "ANN_EMIS");
        Column averageDailyEmissions = new Column(types.realType(), new RealFormatter(), "AVD_EMIS");
        Column ceff = new Column(types.realType(), new RealFormatter(), "CEFF");
        Column reff = new Column(types.realType(), new RealFormatter(), "REFF");
        Column cpri = new Column(types.intType(), new IntegerFormatter(), "CPRI");
        Column csec = new Column(types.intType(), new IntegerFormatter(), "CSEC");

        return new Column[] { fips, plantId, pointId, stackId, segment, plant, scc, erpType, srcType, stkhgt, stkdiam,
                stktemp, stkflow, stkvel, sic, mact, naics, ctype, xloc, yloc, utmz, pollutant, annualEmissions,
                averageDailyEmissions, ceff, reff, cpri, csec };
    }

}
