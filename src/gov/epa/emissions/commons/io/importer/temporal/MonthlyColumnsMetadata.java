package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class MonthlyColumnsMetadata implements ColumnsMetadata {

    private SqlDataTypes types;

    public MonthlyColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "Monthly - Temporal Profile";
    }

    public Column[] cols() {
        Column code = new Column("Code", types.intType(), 5, new IntegerFormatter());
        Column jan = new Column("Jan", types.intType(), 4, new IntegerFormatter());
        Column feb = new Column("Feb", types.intType(), 4, new IntegerFormatter());
        Column mar = new Column("Mar", types.intType(), 4, new IntegerFormatter());
        Column apr = new Column("Apr", types.intType(), 4, new IntegerFormatter());
        Column may = new Column("May", types.intType(), 4, new IntegerFormatter());
        Column jun = new Column("Jun", types.intType(), 4, new IntegerFormatter());
        Column jul = new Column("Jul", types.intType(), 4, new IntegerFormatter());
        Column aug = new Column("Aug", types.intType(), 4, new IntegerFormatter());
        Column sep = new Column("Sep", types.intType(), 4, new IntegerFormatter());
        Column oct = new Column("Oct", types.intType(), 4, new IntegerFormatter());
        Column nov = new Column("Nov", types.intType(), 4, new IntegerFormatter());
        Column dec = new Column("Dec", types.intType(), 4, new IntegerFormatter());
        Column totalWeights = new Column("Total_Weights", types.intType(), 5, new IntegerFormatter());

        return new Column[] { code, jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec, totalWeights };
    }

}
