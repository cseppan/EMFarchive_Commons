package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.OptionalColumnsMetadata;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORLOnRoadColumnsMetadata implements OptionalColumnsMetadata {

    private SqlDataTypes types;

    public ORLOnRoadColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "ORL OnRoad";
    }

    public Column[] cols() {
        return asArray(minCols(), optionalCols());
    }

    private Column[] asArray(Column[] minCols, Column[] optionalCols) {
        List list = new ArrayList();
        list.addAll(Arrays.asList(minCols));
        list.addAll(Arrays.asList(optionalCols));

        return (Column[]) list.toArray(new Column[0]);
    }

    public Column[] minCols() {
        List cols = new ArrayList();

        cols.add(new Column("FIPS", types.intType(), new IntegerFormatter()));
        cols.add(new Column("SCC", types.stringType(10), new StringFormatter(10)));
        cols.add(new Column("POLL", types.stringType(16), new StringFormatter(16)));
        cols.add(new Column("ANN_EMIS", types.realType(), new RealFormatter()));

        return (Column[]) cols.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        List cols = new ArrayList();
        cols.add(new Column("AVD_EMIS", types.realType(), new RealFormatter()));

        return (Column[]) cols.toArray(new Column[0]);
    }

}
