package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

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
        Column spacer1 = new Column("spacer1", types.stringType(1), 1,
                new StringFormatter(1));
        Column cas = new Column("CAS", types.stringType(10),10,
                new StringFormatter(10));
        Column spacer2 = new Column("spacer2", types.stringType(1), 1,
                new StringFormatter(1));
        Column saroad = new Column("SAROAD", types.intType(), 5,
                new IntegerFormatter());
        Column spacer3 = new Column("spacer3", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column react = new Column("REACT", types.intType(), 1,
                new IntegerFormatter());
        Column spacer4 = new Column("spacer4", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column keep = new Column("KEEP", types.stringType(1), 1,
                new StringFormatter(1));
        Column spacer5 = new Column("spacer5", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column factor = new Column("FACTOR", types.realType(), 5,
                new RealFormatter());
        Column spacer6 = new Column("spacer6", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column voctog = new Column("VOCTOG", types.stringType(1), 1,
                new StringFormatter(1));
        Column spacer7 = new Column("spacer7", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column species = new Column("SPECIES", types.stringType(1), 1,
                new StringFormatter(1));
        Column spacer8 = new Column("spacer8", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column explicit = new Column("EXPLICIT", types.stringType(1),1,
                new StringFormatter(1));
        Column spacer9 = new Column("spacer9", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column activity = new Column("ACTIVITY", types.stringType(1), 1,
                new StringFormatter(1));
        Column spacer10 = new Column("spacer10", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column nti = new Column("NTI", types.intType(), 3,
                new IntegerFormatter());
        Column spacer11 = new Column("spacer11", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column units = new Column("UNITS", types.stringType(16), 16,
                new StringFormatter(16));
        Column spacer12 = new Column("spacer12", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column descrptn = new Column("DESCRPTN", types.stringType(40), 40,
                new StringFormatter(40));
        Column spacer13 = new Column("spacer13", types.stringType(spacer), spacer,
                new StringFormatter(spacer));
        Column casdesc = new Column("CASDESC", types.stringType(40), 40,
                new StringFormatter(40));

        return new Column[] { name, spacer1, cas, spacer2, saroad, spacer3,
                react, spacer4, keep, spacer5, factor, spacer6, voctog, spacer7,
                species, spacer8, explicit, spacer9, activity, spacer10, nti, 
                spacer11, units, spacer12, descrptn, spacer13, casdesc};
    }

}
