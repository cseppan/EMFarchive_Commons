package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.SmallIntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class OrlPointColumnsMetadata implements ORLColumnsMetadata {

    private String[] colTypes;

    private String[] colNames;

    public OrlPointColumnsMetadata(SqlDataTypes types) {
        String intType = types.intType();
        colTypes = new String[] { intType, types.stringType(15), types.stringType(15), types.stringType(15),
                types.stringType(15), types.stringType(40), types.stringType(10), types.stringType(2),
                types.stringType(2), types.realType(), types.realType(), types.realType(), types.realType(),
                types.realType(), types.stringType(4), types.stringType(6), types.stringType(6), types.stringType(1),
                types.realType(), types.realType(), types.smallInt(), types.stringType(16), types.realType(),
                types.realType(), types.realType(), types.realType(), intType, intType };

        colNames = new String[] { "FIPS", "PLANTID", "POINTID", "STACKID", "SEGMENT", "PLANT", "SCC", "ERPTYPE",
                "SRCTYPE", "STKHGT", "STKDIAM", "STKTEMP", "STKFLOW", "STKVEL", "SIC", "MACT", "NAICS", "CTYPE",
                "XLOC", "YLOC", "UTMZ", "POLL", "ANN_EMIS", "AVD_EMIS", "CEFF", "REFF", "CPRI", "CSEC" };
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
        return "ORL Point";
    }

    public Column[] cols() {
        Column fips = new Column(new IntegerFormatter(), "FIPS");
        Column plantId = new Column(new StringFormatter(15), "PLANTID");
        Column pointId = new Column(new StringFormatter(15), "POINTID");
        Column stackId = new Column(new StringFormatter(15), "STACKID");
        Column segment = new Column(new StringFormatter(15), "SEGMENT");
        Column plant = new Column(new StringFormatter(40), "PLANT");
        Column scc = new Column(new StringFormatter(10), "SCC");
        Column erpType = new Column(new StringFormatter(2), "ERPTYPE");
        Column srcType = new Column(new StringFormatter(2), "SRCTYPE");
        Column stkhgt = new Column(new RealFormatter(), "STKHGT");
        Column stkdiam = new Column(new RealFormatter(), "STKDIAM");
        Column stktemp = new Column(new RealFormatter(), "STKTEMP");
        Column stkflow = new Column(new RealFormatter(), "STKFLOW");
        Column stkvel = new Column(new RealFormatter(), "STKVEL");
        Column sic = new Column(new StringFormatter(4), "SIC");
        Column mact = new Column(new StringFormatter(6), "MACT");
        Column naics = new Column(new StringFormatter(6), "NAICS");
        Column ctype = new Column(new StringFormatter(1), "CTYPE");
        Column xloc = new Column(new RealFormatter(), "XLOC");
        Column yloc = new Column(new RealFormatter(), "YLOC");
        Column utmz = new Column(new SmallIntegerFormatter(), "UTMZ");
        Column pollutant = new Column(new StringFormatter(16), "POLL");
        Column annualEmissions = new Column(new RealFormatter(), "ANN_EMIS");
        Column averageDailyEmissions = new Column(new RealFormatter(), "AVD_EMIS");
        Column ceff = new Column(new RealFormatter(), "CEFF");
        Column reff = new Column(new RealFormatter(), "REFF");
        Column cpri = new Column(new IntegerFormatter(), "CPRI");
        Column csec = new Column(new IntegerFormatter(), "CSEC");

        return new Column[] { fips, plantId, pointId, stackId, segment, plant, scc, erpType, srcType, stkhgt, stkdiam,
                stktemp, stkflow, stkvel, sic, mact, naics, ctype, xloc, yloc, utmz, pollutant, annualEmissions,
                averageDailyEmissions, ceff, reff, cpri, csec };
    }

}
