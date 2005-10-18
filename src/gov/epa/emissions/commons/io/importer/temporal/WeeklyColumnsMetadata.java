package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

import java.util.ArrayList;
import java.util.List;

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
