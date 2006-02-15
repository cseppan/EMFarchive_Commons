package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.SqlDataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NonVersionedTableFormat implements TableFormat {

    private FileFormat base;

    private SqlDataTypes types;

    public NonVersionedTableFormat(FileFormat base, SqlDataTypes types) {
        this.base = base;
        this.types = types;
    }

    public String key() {
        return "Dataset_Id";
    }

    public String identify() {
        return base.identify();
    }

    public Column[] cols() {
        List cols = new ArrayList();
        cols.addAll(Arrays.asList(base.cols()));

        Column datasetId = new Column(key(), types.longType(), new LongFormatter());
        cols.add(0, datasetId);

        Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
        cols.add(inlineComments);

        return (Column[]) cols.toArray(new Column[0]);
    }

}
