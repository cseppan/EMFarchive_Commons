package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MobileTemporalReferenceFileFormat implements FileFormatWithOptionalCols {
    private SqlDataTypes types;

    public MobileTemporalReferenceFileFormat(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "Mobile - Temporal Reference";
    }

    public Column[] cols() {
        Column[] minCols = minCols();
        Column[] optionalCols = optionalCols();

        return asArray(minCols, optionalCols);
    }

    private Column[] asArray(Column[] minCols, Column[] optionalCols) {
        List list = new ArrayList();
        list.addAll(Arrays.asList(minCols));
        list.addAll(Arrays.asList(optionalCols));

        return (Column[]) list.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        Column pollutants = new Column("Pollutants", types.stringType(32), new StringFormatter(32));
        Column fips = new Column("FIPS", types.intType(), new IntegerFormatter());
        Column linkid = new Column("Link_Id", types.stringType(32), new StringFormatter(32));

        return new Column[] { pollutants, fips, linkid };
    }

    public Column[] minCols() {
        Column scc = new Column("SCC", types.stringType(10), new StringFormatter(10));
        Column monthlyCode = new Column("Monthly_Code", types.intType(), new IntegerFormatter());
        Column weeklyCode = new Column("Weekly_Code", types.intType(), new IntegerFormatter());
        Column diurnalCode = new Column("Diurnal_Code", types.intType(), new IntegerFormatter());

        return new Column[] { scc, monthlyCode, weeklyCode, diurnalCode };
    }
}
