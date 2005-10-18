package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class DiurnalColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private SqlDataTypes types;

    public DiurnalColumnsMetadata(SqlDataTypes types) {
        this.types = types;
        widths = new int[] { 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5 };
    }

    public int[] widths() {
        return widths;
    }

    public String identify() {
        return "Diurnal - Temporal Profile";
    }

    public Column[] cols() {
        Column code = new Column(types.intType(), new IntegerFormatter(), "Code");
        Column hr0 = new Column(types.intType(), new IntegerFormatter(), "hr0");
        Column hr1 = new Column(types.intType(), new IntegerFormatter(), "hr1");
        Column hr2 = new Column(types.intType(), new IntegerFormatter(), "hr2");
        Column hr3 = new Column(types.intType(), new IntegerFormatter(), "hr3");
        Column hr4 = new Column(types.intType(), new IntegerFormatter(), "hr4");
        Column hr5 = new Column(types.intType(), new IntegerFormatter(), "hr5");
        Column hr6 = new Column(types.intType(), new IntegerFormatter(), "hr6");
        Column hr7 = new Column(types.intType(), new IntegerFormatter(), "hr7");
        Column hr8 = new Column(types.intType(), new IntegerFormatter(), "hr8");
        Column hr9 = new Column(types.intType(), new IntegerFormatter(), "hr9");
        Column hr10 = new Column(types.intType(), new IntegerFormatter(), "hr10");
        Column hr11 = new Column(types.intType(), new IntegerFormatter(), "hr11");
        Column hr12 = new Column(types.intType(), new IntegerFormatter(), "hr12");
        Column hr13 = new Column(types.intType(), new IntegerFormatter(), "hr13");
        Column hr14 = new Column(types.intType(), new IntegerFormatter(), "hr14");
        Column hr15 = new Column(types.intType(), new IntegerFormatter(), "hr15");
        Column hr16 = new Column(types.intType(), new IntegerFormatter(), "hr16");
        Column hr17 = new Column(types.intType(), new IntegerFormatter(), "hr17");
        Column hr18 = new Column(types.intType(), new IntegerFormatter(), "hr18");
        Column hr19 = new Column(types.intType(), new IntegerFormatter(), "hr19");
        Column hr20 = new Column(types.intType(), new IntegerFormatter(), "hr20");
        Column hr21 = new Column(types.intType(), new IntegerFormatter(), "hr21");
        Column hr22 = new Column(types.intType(), new IntegerFormatter(), "hr22");
        Column hr23 = new Column(types.intType(), new IntegerFormatter(), "hr23");
        Column totalWeights = new Column(types.intType(), new IntegerFormatter(), "Total_Weights");

        return new Column[] { code, hr0, hr1, hr2, hr3, hr4, hr5, hr6, hr7, hr8, hr9, hr10, hr11, hr12, hr13, hr14,
                hr15, hr16, hr17, hr18, hr19, hr20, hr21, hr22, hr23, totalWeights };
    }

}
