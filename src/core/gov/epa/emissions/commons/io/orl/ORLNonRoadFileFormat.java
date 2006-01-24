package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;
import gov.epa.emissions.commons.io.importer.FillRecordWithBlankValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORLNonRoadFileFormat implements FileFormatWithOptionalCols {

    private SqlDataTypes types;

    private FillDefaultValues filler;

    public ORLNonRoadFileFormat(SqlDataTypes types) {
        this(types, new FillRecordWithBlankValues());
    }

    public ORLNonRoadFileFormat(SqlDataTypes types, FillDefaultValues filler) {
        this.types = types;
        this.filler = filler;
    }

    public String identify() {
        return "ORL NonRoad";
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

        cols.add(new Column("FIPS", types.stringType(32), new StringFormatter(32)));
        cols.add(new Column("SCC", types.stringType(10), new StringFormatter(10)));
        cols.add(new Column("POLL", types.stringType(16), new StringFormatter(16)));
        cols.add(new Column("ANN_EMIS", types.realType(), new RealFormatter()));

        return (Column[]) cols.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        List cols = new ArrayList();

        cols.add(new Column("AVD_EMIS", types.realType(), new RealFormatter()));
        cols.add(new Column("CEFF", types.realType(), new RealFormatter()));
        cols.add(new Column("REFF", types.realType(), new RealFormatter()));
        cols.add(new Column("RPEN", types.realType(), new RealFormatter()));
        // extended orl columns
        cols.add(new Column("SRCTYPE", types.stringType(2), new StringFormatter(2)));
        cols.add(new Column("DATA_SOURCE", types.stringType(10), new StringFormatter(10)));
        cols.add(new Column("YEAR", types.stringType(4), new StringFormatter(4)));
        cols.add(new Column("TRIBAL_CODE", types.stringType(3), new StringFormatter(3)));
        
        cols.add(new Column("START_DATE", types.stringType(8), new StringFormatter(8)));
        cols.add(new Column("END_DATE", types.stringType(8), new StringFormatter(8)));
        cols.add(new Column("WINTER_THROUGHPUT_PCT", types.realType(), new RealFormatter()));
        cols.add(new Column("SPRING_THROUGHPUT_PCT", types.realType(), new RealFormatter()));
        cols.add(new Column("SUMMER_THROUGHPUT_PCT", types.realType(), new RealFormatter()));
        cols.add(new Column("FALL_THROUGHPUT_PCT", types.realType(), new RealFormatter()));
        cols.add(new Column("ANNUAL_AVG_DAYS_PER_WEEK", types.realType(), new RealFormatter()));
        cols.add(new Column("ANNUAL_AVG_WEEKS_PER_YEAR", types.realType(), new RealFormatter()));
        cols.add(new Column("ANNUAL_AVG_HOURS_PER_DAY", types.realType(), new RealFormatter()));
        cols.add(new Column("ANNUAL_AVG_HOURS_PER_YEAR", types.realType(), new RealFormatter()));
        cols.add(new Column("PERIOD_DAYS_PER_WEEK", types.realType(), new RealFormatter()));
        cols.add(new Column("PERIOD_WEEKS_PER_PERIOD", types.realType(), new RealFormatter()));
        cols.add(new Column("PERIOD_HOURS_PER_DAY", types.realType(), new RealFormatter()));
        cols.add(new Column("PERIOD_HOURS_PER_PERIOD", types.realType(), new RealFormatter()));
        
        return (Column[]) cols.toArray(new Column[0]);
    }

    public void fillDefaults(List data, long datasetId) {
        filler.fill(this, data, datasetId);
    }

}
