package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class ORLDaySpecFiresFileFormat implements FileFormat, DelimitedFileFormat {
    private SqlDataTypes types;

    public ORLDaySpecFiresFileFormat(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "ORL Day-Specific Fires Inventory";
    }

    public Column[] cols() {
        Column fips = new Column("FIPS", types.stringType(6), 6, new StringFormatter(6));
        Column fireId = new Column("FIREID", types.stringType(15), 15, new StringFormatter(15));
        Column locId = new Column("LOCID", types.stringType(15), 15, new StringFormatter(15));
        Column scc = new Column("SCC", types.stringType(10), 10, new StringFormatter(10));
        Column dat = new Column("DATA", types.stringType(16), 16, new StringFormatter(16));
        Column date = new Column("DATE", types.stringType(8), 8, new StringFormatter(8));
        Column datVal = new Column("DATAVALUE", types.realType(), new RealFormatter());
        Column beginHr = new Column("BEGHOUR", types.intType(), new IntegerFormatter());
        Column endHr = new Column("ENDHOUR", types.intType(), new IntegerFormatter());

        return new Column[] { fips, fireId, locId, scc, dat, date, datVal, beginHr, endHr };
    }
}
