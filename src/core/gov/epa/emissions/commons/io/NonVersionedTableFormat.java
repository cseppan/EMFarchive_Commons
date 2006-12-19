package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.SqlDataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NonVersionedTableFormat implements TableFormat {

    private FileFormat base;

    private SqlDataTypes types;
    
    private Column[] cols;

    public NonVersionedTableFormat(FileFormat base, SqlDataTypes types) {
        this.base = base;
        this.types = types;
        this.cols = createCols();
    }

    public NonVersionedTableFormat(FileFormat base, SqlDataTypes types, String lineNum) {
        this.base = base;
        this.types = types;
        this.cols =createCols(lineNum); //add a column called "lineNum"
    }

    public String key() {
        return "Dataset_Id";
    }

    public String identify() {
        return base.identify();
    }

    public Column[] cols() {
        return this.cols;
    }

    private Column[] createCols() {
        List cols = new ArrayList();
        cols.addAll(Arrays.asList(base.cols()));

        Column datasetId = new Column(key(), types.longType(), new LongFormatter());
        cols.add(0, datasetId);

        Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
        cols.add(inlineComments);

        return (Column[]) cols.toArray(new Column[0]);
    }

    private Column[] createCols(String lineNum) {
        List cols = new ArrayList();
        cols.add(new Column(lineNum, types.realType(), new RealFormatter())); //add line number column
        cols.addAll(Arrays.asList(base.cols()));
        
        Column datasetId = new Column(key(), types.longType(), new LongFormatter());
        cols.add(0, datasetId);
        
        Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
        cols.add(inlineComments);
        
        return (Column[]) cols.toArray(new Column[0]);
    }

}
