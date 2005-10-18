package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class WeeklyColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private SqlDataTypes types;

    public WeeklyColumnsMetadata(SqlDataTypes types) {
        this.types = types;
        widths = new int[] { 5, 4, 4, 4, 4, 4, 4, 4, 5 };
    }

    public int[] widths() {
        return widths;
    }

    public String identify() {
        return "Weekly - Temporal Profile";
    }

    public Column[] cols() {
        Column code = new Column(types.intType(), new IntegerFormatter(), "Code");
        Column mon = new Column(types.intType(), new IntegerFormatter(), "Mon");
        Column tue = new Column(types.intType(), new IntegerFormatter(), "Tue");
        Column wed = new Column(types.intType(), new IntegerFormatter(), "Wed");
        Column thu = new Column(types.intType(), new IntegerFormatter(), "Thu");
        Column fri = new Column(types.intType(), new IntegerFormatter(), "Fri");
        Column sat = new Column(types.intType(), new IntegerFormatter(), "Sat");
        Column sun = new Column(types.intType(), new IntegerFormatter(), "Sun");
        Column totalWeights = new Column(types.intType(), new IntegerFormatter(), "Total_Weights");

        return new Column[] { code, mon, tue, wed, thu, fri, sat, sun, totalWeights };
    }

}
