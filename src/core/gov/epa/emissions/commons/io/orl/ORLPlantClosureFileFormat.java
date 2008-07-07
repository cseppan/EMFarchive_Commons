package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;
import gov.epa.emissions.commons.io.importer.FillRecordWithBlankValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORLPlantClosureFileFormat implements FileFormatWithOptionalCols, DelimitedFileFormat {

    protected SqlDataTypes types;

    private FillDefaultValues filler;

    public ORLPlantClosureFileFormat(SqlDataTypes types) {
        this(types, new FillRecordWithBlankValues());
    }

    public ORLPlantClosureFileFormat(SqlDataTypes types, FillDefaultValues filler) {
        this.types = types;
        this.filler = filler;
    }

    public String identify() {
        return "ORL Point";
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
        List<Column> cols = new ArrayList<Column>();

        cols.add(new Column("FIPS", types.stringType(6), 6, new StringFormatter(6)));
        cols.add(new Column("PLANTID", types.stringType(15), 15, new StringFormatter(15)));
        
        return cols.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        List<Column> cols = new ArrayList<Column>();

        cols.add(new Column("POINTID", types.stringType(15), 15, new StringFormatter(15)));
        cols.add(new Column("STACKID", types.stringType(15), 15, new StringFormatter(15)));
        cols.add(new Column("SEGMENT", types.stringType(15), 15, new StringFormatter(15)));
        cols.add(new Column("PLANT", types.stringType(40), 40, new StringFormatter(40)));
        cols.add(new Column("EFFECTIVE_DATE", types.timestamp()));
        cols.add(new Column("REFERENCE", types.stringType(128), 128, new StringFormatter(128)));

        return cols.toArray(new Column[0]);
    }

    public void fillDefaults(List<Column> data, long datasetId) {
        filler.fill(this, data, datasetId);
    }

}
