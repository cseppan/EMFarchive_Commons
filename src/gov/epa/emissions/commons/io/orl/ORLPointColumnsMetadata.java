package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.OptionalColumnsMetadata;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.SmallIntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORLPointColumnsMetadata implements OptionalColumnsMetadata {

    private SqlDataTypes types;

    public ORLPointColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "ORL Point";
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
        cols.add(new Column("PLANTID", types.stringType(15), new StringFormatter(15)));
        cols.add(new Column("POINTID", types.stringType(15), new StringFormatter(15)));
        cols.add(new Column("STACKID", types.stringType(15), new StringFormatter(15)));
        cols.add(new Column("SEGMENT", types.stringType(15), new StringFormatter(15)));
        cols.add(new Column("PLANT", types.stringType(40), new StringFormatter(40)));
        cols.add(new Column("SCC", types.stringType(10), new StringFormatter(10)));
        cols.add(new Column("ERPTYPE", types.stringType(2), new StringFormatter(2)));
        cols.add(new Column("SRCTYPE", types.stringType(2), new StringFormatter(2)));
        cols.add(new Column("STKHGT", types.realType(), new RealFormatter()));
        cols.add(new Column("STKDIAM", types.realType(), new RealFormatter()));
        cols.add(new Column("STKTEMP", types.realType(), new RealFormatter()));
        cols.add(new Column("STKFLOW", types.realType(), new RealFormatter()));
        cols.add(new Column("STKVEL", types.realType(), new RealFormatter()));
        cols.add(new Column("SIC", types.stringType(4), new StringFormatter(4)));
        cols.add(new Column("MACT", types.stringType(6), new StringFormatter(6)));
        cols.add(new Column("NAICS", types.stringType(6), new StringFormatter(6)));
        cols.add(new Column("CTYPE", types.stringType(1), new StringFormatter(1)));
        cols.add(new Column("XLOC", types.realType(), new RealFormatter()));
        cols.add(new Column("YLOC", types.realType(), new RealFormatter()));
        cols.add(new Column("UTMZ", types.smallInt(), new SmallIntegerFormatter()));
        cols.add(new Column("POLL", types.stringType(16), new StringFormatter(16)));

        return (Column[]) cols.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        List cols = new ArrayList();

        cols.add(new Column("ANN_EMIS", types.realType(), new RealFormatter()));
        cols.add(new Column("AVD_EMIS", types.realType(), new RealFormatter()));
        cols.add(new Column("CEFF", types.realType(), new RealFormatter()));
        cols.add(new Column("REFF", types.realType(), new RealFormatter()));
        cols.add(new Column("CPRI", types.intType(), new IntegerFormatter()));
        cols.add(new Column("CSEC", types.intType(), new IntegerFormatter()));

        return (Column[]) cols.toArray(new Column[0]);
    }

}
