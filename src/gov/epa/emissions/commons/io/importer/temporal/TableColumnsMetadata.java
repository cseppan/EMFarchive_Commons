package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableColumnsMetadata implements ColumnsMetadata {

    private ColumnsMetadata base;

    private SqlDataTypes types;

    public TableColumnsMetadata(ColumnsMetadata base, SqlDataTypes types) {
        this.base = base;
        this.types = types;
    }

    public int[] widths() {
        return base.widths();
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

        Column datasetId = new Column(types.longType(), new LongFormatter(), key());
        cols.add(0, datasetId);

        Column inlineComments = new Column(types.stringType(128), new StringFormatter(128), "Comments");
        cols.add(inlineComments);

        return (Column[]) cols.toArray(new Column[0]);
    }

}
