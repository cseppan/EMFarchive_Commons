package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class InventoryTableFileFormat implements FileFormat {
    private Column[] cols;   

    public InventoryTableFileFormat(SqlDataTypes types, int spacer) {
        cols = createCols(types, spacer);
    }

    public String identify() {
        return "Inventory Table Data";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types, int spacer) {
        if(spacer < 0)
            spacer = 0;
        Column name = new Column("NAME", types.stringType(11), 11,
                new StringFormatter(11));
        Column cas = new Column("CAS", types.stringType(11),11,
                new StringFormatter(11));
        Column saroad = new Column("SAROAD", types.intType(), 6,
                new IntegerFormatter());
        Column react = new Column("REACT", types.intType(), 1 + spacer,
                new IntegerFormatter());
        Column keep = new Column("KEEP", types.stringType(1 + spacer), 1 + spacer,
                new StringFormatter(1 + spacer));
        Column factor = new Column("FACTOR", types.realType(), 5 + spacer,
                new RealFormatter());
        Column voctog = new Column("VOCTOG", types.stringType(1 + spacer), 1 + spacer,
                new StringFormatter(1 + spacer));
        Column species = new Column("SPECIES", types.stringType(1 + spacer), 1 + spacer,
                new StringFormatter(1 + spacer));
        Column explicit = new Column("EXPLICIT", types.stringType(1 + spacer),1 + spacer,
                new StringFormatter(1 + spacer));
        Column activity = new Column("ACTIVITY", types.stringType(1 + spacer), 1 + spacer,
                new StringFormatter(1 + spacer));
        Column nti = new Column("NTI", types.intType(), 3 + spacer,
                new IntegerFormatter());
        Column units = new Column("UNITS", types.stringType(16 + spacer), 16 + spacer,
                new StringFormatter(16 + spacer));
        Column descrptn = new Column("DESCRPTN", types.stringType(40 + spacer), 40 + spacer,
                new StringFormatter(40 + spacer));
        Column casdesc = new Column("CASDESC", types.stringType(40 + spacer), 40 + spacer,
                new StringFormatter(40 + spacer));

        return new Column[] { name, cas, saroad, react, keep, factor, voctog, species, 
                explicit, activity, nti, units, descrptn, casdesc};
    }

}
