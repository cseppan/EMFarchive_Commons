package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.SmallIntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class ORLPointColumnsMetadata implements ColumnsMetadata {

    private SqlDataTypes types;

    public ORLPointColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "ORL Point";
    }

    public Column[] cols() {
        Column fips = new Column("FIPS", types.intType(), new IntegerFormatter());
        Column plantId = new Column("PLANTID", types.stringType(15), new StringFormatter(15));
        Column pointId = new Column("POINTID", types.stringType(15), new StringFormatter(15));
        Column stackId = new Column("STACKID", types.stringType(15), new StringFormatter(15));
        Column segment = new Column("SEGMENT", types.stringType(15), new StringFormatter(15));
        Column plant = new Column("PLANT", types.stringType(40), new StringFormatter(40));
        Column scc = new Column("SCC", types.stringType(10), new StringFormatter(10));
        Column erpType = new Column("ERPTYPE", types.stringType(2), new StringFormatter(2));
        Column srcType = new Column("SRCTYPE", types.stringType(2), new StringFormatter(2));
        Column stkhgt = new Column("STKHGT", types.realType(), new RealFormatter());
        Column stkdiam = new Column("STKDIAM", types.realType(), new RealFormatter());
        Column stktemp = new Column("STKTEMP", types.realType(), new RealFormatter());
        Column stkflow = new Column("STKFLOW", types.realType(), new RealFormatter());
        Column stkvel = new Column("STKVEL", types.realType(), new RealFormatter());
        Column sic = new Column("SIC", types.stringType(4), new StringFormatter(4));
        Column mact = new Column("MACT", types.stringType(6), new StringFormatter(6));
        Column naics = new Column("NAICS", types.stringType(6), new StringFormatter(6));
        Column ctype = new Column("CTYPE", types.stringType(1), new StringFormatter(1));
        Column xloc = new Column("XLOC", types.realType(), new RealFormatter());
        Column yloc = new Column("YLOC", types.realType(), new RealFormatter());
        Column utmz = new Column("UTMZ", types.smallInt(), new SmallIntegerFormatter());
        Column pollutant = new Column("POLL", types.stringType(16), new StringFormatter(16));
        Column annualEmissions = new Column("ANN_EMIS", types.realType(), new RealFormatter());
        Column averageDailyEmissions = new Column("AVD_EMIS", types.realType(), new RealFormatter());
        Column ceff = new Column("CEFF", types.realType(), new RealFormatter());
        Column reff = new Column("REFF", types.realType(), new RealFormatter());
        Column cpri = new Column("CPRI", types.intType(), new IntegerFormatter());
        Column csec = new Column("CSEC", types.intType(), new IntegerFormatter());

        return new Column[] { fips, plantId, pointId, stackId, segment, plant, scc, erpType, srcType, stkhgt, stkdiam,
                stktemp, stkflow, stkvel, sic, mact, naics, ctype, xloc, yloc, utmz, pollutant, annualEmissions,
                averageDailyEmissions, ceff, reff, cpri, csec };
    }

}
