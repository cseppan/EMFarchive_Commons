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

    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String[] colTypes() {
        Column[] cols = cols();

        List sqlTypes = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            sqlTypes.add(cols[i].sqlType());
        }

        return (String[]) sqlTypes.toArray(new String[0]);
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
