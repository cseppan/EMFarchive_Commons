package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class WeeklyColumnsMetadata implements ColumnsMetadata {

    private SqlDataTypes types;

    public WeeklyColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "Weekly - Temporal Profile";
    }

    public Column[] cols() {
        Column code = new Column("Code", types.intType(), 5, new IntegerFormatter());
        Column mon = new Column("Mon", types.intType(), 4, new IntegerFormatter());
        Column tue = new Column("Tue", types.intType(), 4, new IntegerFormatter());
        Column wed = new Column("Wed", types.intType(), 4, new IntegerFormatter());
        Column thu = new Column("Thu", types.intType(), 4, new IntegerFormatter());
        Column fri = new Column("Fri", types.intType(), 4, new IntegerFormatter());
        Column sat = new Column("Sat", types.intType(), 4, new IntegerFormatter());
        Column sun = new Column("Sun", types.intType(), 4, new IntegerFormatter());
        Column totalWeights = new Column("Total_Weights", types.intType(), 5, new IntegerFormatter());

        return new Column[] { code, mon, tue, wed, thu, fri, sat, sun, totalWeights };
    }

}
