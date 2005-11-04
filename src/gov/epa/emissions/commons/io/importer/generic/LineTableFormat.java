package gov.epa.emissions.commons.io.importer.generic;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LineTableFormat implements TableFormat {
    private FileFormat base;

    private Column[] cols;

    public LineTableFormat(FileFormat base, SqlDataTypes types) {
        this.base = base;
        cols = createCols(types);
    }

    public String key() {
        return "Dataset_Id";
    }

    public Column[] cols() {
        return cols;
    }
    
    public int numColumns(){
        return cols.length;
    }

    private Column[] createCols(SqlDataTypes types) {
        List cols = new ArrayList();
        cols.addAll(Arrays.asList(base.cols()));

        Column datasetId = new Column(key(), types.longType(),
                new LongFormatter());
        cols.add(0, datasetId);
        
        return (Column[]) cols.toArray(new Column[0]);
    }

    public String identify() {
        return base.identify();
    }

}
