package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class MonthlyColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private SqlDataTypes types;

    public MonthlyColumnsMetadata(SqlDataTypes types) {
        this.types = types;
        widths = new int[] { 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5 };
    }

    public int[] widths() {
        return widths;
    }

    public String identify() {
        return "Monthly - Temporal Profile";
    }

    public Column[] cols() {
        Column code = new Column(types.intType(), new IntegerFormatter(), "Code");
        Column jan = new Column(types.intType(), new IntegerFormatter(), "Jan");
        Column feb = new Column(types.intType(), new IntegerFormatter(), "Feb");
        Column mar = new Column(types.intType(), new IntegerFormatter(), "Mar");
        Column apr = new Column(types.intType(), new IntegerFormatter(), "Apr");
        Column may = new Column(types.intType(), new IntegerFormatter(), "May");
        Column jun = new Column(types.intType(), new IntegerFormatter(), "Jun");
        Column jul = new Column(types.intType(), new IntegerFormatter(), "Jul");
        Column aug = new Column(types.intType(), new IntegerFormatter(), "Aug");
        Column sep = new Column(types.intType(), new IntegerFormatter(), "Sep");
        Column oct = new Column(types.intType(), new IntegerFormatter(), "Oct");
        Column nov = new Column(types.intType(), new IntegerFormatter(), "Nov");
        Column dec = new Column(types.intType(), new IntegerFormatter(), "Dec");
        Column totalWeights = new Column(types.intType(), new IntegerFormatter(), "Total_Weights");

        return new Column[] { code, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec, totalWeights };
    }

}
