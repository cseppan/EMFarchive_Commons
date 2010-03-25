package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.postgres.PostgresSqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.NullFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;
import gov.epa.emissions.commons.io.importer.FillRecordWithBlankValues;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORLNonPointFileFormat implements FileFormatWithOptionalCols, DelimitedFileFormat {

    private SqlDataTypes types;

    private FillDefaultValues filler;

    private Column[] minCols;

    private Column[] optionalCols;

    public ORLNonPointFileFormat(SqlDataTypes types) {
        this(types, new FillRecordWithBlankValues());
    }

    public ORLNonPointFileFormat(SqlDataTypes types, FillDefaultValues filler) {
        this.types = types;
        this.filler = filler;
        this.minCols = createMinCols();
        this.optionalCols = createOptionalCols();
    }

    public String identify() {
        return "ORL NonPoint";
    }

    public Column[] cols() {
        return asArray(minCols, optionalCols);
    }

    private Column[] asArray(Column[] minCols, Column[] optionalCols) {
        List<Column> list = new ArrayList<Column>();
        list.addAll(Arrays.asList(minCols));
        list.addAll(Arrays.asList(optionalCols));

        return list.toArray(new Column[0]);
    }

    public Column[] minCols() {
        return minCols;
    }

    public Column[] optionalCols() {
        return optionalCols;
    }

    private Column[] createMinCols() {
        List<Column> cols = new ArrayList<Column>();

        cols.add(new Column("FIPS", types.stringType(6), 6, new StringFormatter(6)));
        cols.add(new Column("SCC", types.stringType(10), 10, new StringFormatter(10)));
        cols.add(new Column("SIC", types.stringType(4), 4, new StringFormatter(4)));
        cols.add(new Column("MACT", types.stringType(6), 6, new StringFormatter(6)));
        cols.add(new Column("SRCTYPE", types.stringType(2), 2, new StringFormatter(2)));
        cols.add(new Column("NAICS", types.stringType(6), 6, new StringFormatter(6)));
        cols.add(new Column("POLL", types.stringType(16), 16, new StringFormatter(16)));
        cols.add(new Column("ANN_EMIS", types.realType(), new RealFormatter()));

        return cols.toArray(new Column[0]);
    }

    private Column[] createOptionalCols() {
        List<Column> cols = new ArrayList<Column>();

        cols.add(new Column("AVD_EMIS", types.realType(), new RealFormatter()));
        cols.add(new Column("CEFF", types.realType(), new RealFormatter()));
        cols.add(new Column("REFF", types.realType(), new RealFormatter()));
        cols.add(new Column("RPEN", types.realType(), new RealFormatter()));
        // extended orl columns
        cols.add(new Column("PRIMARY_DEVICE_TYPE_CODE", types.stringType(4), 4, new StringFormatter(4)));
        cols.add(new Column("SECONDARY_DEVICE_TYPE_CODE", types.stringType(4), 4, new StringFormatter(4)));
        cols.add(new Column("DATA_SOURCE", types.stringType(10), 10, new StringFormatter(10)));
        cols.add(new Column("YEAR", types.stringType(4), 4, new StringFormatter(4)));
        cols.add(new Column("TRIBAL_CODE", types.stringType(3), 3, new StringFormatter(3)));
        // columns in extended orl but not for SMOKE use
        cols.add(new Column("MACT_FLAG", types.stringType(15), 15, new StringFormatter(15)));
        cols.add(new Column("PROCESS_MACT_COMPLIANCE_STATUS", types.stringType(6), 6, new StringFormatter(8)));
        cols.add(new Column("START_DATE", types.stringType(10), 10, new StringFormatter(10)));
        cols.add(new Column("END_DATE", types.stringType(10), 10, new StringFormatter(10)));
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

        //new columns used for control strategy runs...
        cols.add(new Column("CONTROL_MEASURES", types.text(), new NullFormatter()));
        cols.add(new Column("PCT_REDUCTION", types.text(), new NullFormatter()));
        cols.add(new Column("CURRENT_COST", types.realType(), new RealFormatter()));
        cols.add(new Column("CUMULATIVE_COST", types.realType(), new RealFormatter()));

        return cols.toArray(new Column[0]);
    }

    public void fillDefaults(List<Column> data, long datasetId) {
        filler.fill(this, data, datasetId);
    }
    
    public static void main(String[] args) {
        ORLNonPointFileFormat format = new ORLNonPointFileFormat(new PostgresSqlDataTypes());
        PrintWriter writer = null;
        Column[] mincols = format.minCols();
        Column[] optcols = format.optionalCols();
        
        try {
            writer = new PrintWriter(new File("D:\\emf\\orl_nonpoint_format.csv"));
            writer.println("name,type,default value,description,formatter,constraints,mandatory,width,spaces,fixformat start,fixformat end");
            
            for (Column col : mincols)
                writer.println(col.getName()+","+col.getSqlType()+","+","+","+col.getFormatterClass()+","+","+"true,"
                        +col.getWidth()+","+col.getSpaces()+","+"0,"+"0");
            
            for (Column col : optcols)
                writer.println(col.getName()+","+col.getSqlType()+","+","+","+col.getFormatterClass()+","+","+"false,"
                        +col.getWidth()+","+col.getSpaces()+","+"0,"+"0");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null)
                writer.close();
        }
    }
}
