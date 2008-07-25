package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;
import gov.epa.emissions.commons.io.importer.FillRecordWithBlankValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProjectionPacketFileFormat implements FileFormatWithOptionalCols, DelimitedFileFormat {
    
    private FillDefaultValues filler;
    
    private SqlDataTypes types;
    
    public ProjectionPacketFileFormat(SqlDataTypes types) {
        this(types, new FillRecordWithBlankValues());
    }
    
    public ProjectionPacketFileFormat(SqlDataTypes types, FillDefaultValues filler){
        this.types = types;
        this.filler = filler;
    }
    
    public String identify() {
        return "Projection Packet File";
    }

    public Column[] cols() {
        return asArray(minCols(), optionalCols());
    }

    private Column[] asArray(Column[] minCols, Column[] optionalCols) {
        List<Column> list = new ArrayList<Column>();
        list.addAll(Arrays.asList(minCols));
        list.addAll(Arrays.asList(optionalCols));

        return list.toArray(new Column[0]);
    }
    
    public Column[] minCols() {
        List<Column> columns = new ArrayList<Column>();

        columns.add(new Column("FIPS", types.intType(), new IntegerFormatter()));
        columns.add(new Column("SCC", types.stringType(10), 10, new StringFormatter(10)));
        columns.add(new Column("PROJ_FACTOR", types.realType(), new RealFormatter()));
        columns.add(new Column("POLLUTANT", types.stringType(128), 128, new StringFormatter(128)));
        
        return columns.toArray(new Column[0]);
    }
    
    public Column[] optionalCols() {
        List<Column> cols = new ArrayList<Column>();

        cols.add(new Column("SIC", types.intType(), new IntegerFormatter()));
        cols.add(new Column("MACT", types.stringType(6), 6, new StringFormatter(6)));
        cols.add(new Column("PLANTID", types.stringType(32), 32, new StringFormatter(32)));
        cols.add(new Column("POINTID", types.stringType(32), 32, new StringFormatter(32)));
        cols.add(new Column("STACKID", types.stringType(32), 32, new StringFormatter(32)));
        cols.add(new Column("SEGMENTID", types.stringType(32), 32, new StringFormatter(32)));
        cols.add(new Column("CHAR_4", types.stringType(32), 32, new StringFormatter(32)));
        cols.add(new Column("CHAR_5", types.stringType(32), 32, new StringFormatter(32)));
        
        return cols.toArray(new Column[0]);
    }
    
    public void fillDefaults(List<Column> data, long datasetId) {
        filler.fill(this, data, datasetId);
    }
    
}
