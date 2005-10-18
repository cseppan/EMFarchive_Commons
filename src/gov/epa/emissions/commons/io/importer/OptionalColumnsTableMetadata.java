package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptionalColumnsTableMetadata implements ColumnsMetadata {

    private OptionalColumnsMetadata base;

    private SqlDataTypes types;

    public OptionalColumnsTableMetadata(OptionalColumnsMetadata base, SqlDataTypes types) {
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

    public String key() {
        return "Dataset_Id";
    }

    public int[] widths() {
        return null;
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

    // FIXME: rework this mess
    public void addDefaultValuesForOptionals(List data) {
        if (!includesComment(data))
            addDefaultValuesWithComment(data);
        else
            addDefaultValues(data);
    }

    private void addDefaultValues(List data) {
        int optionalCount = optionalCount(data);
        int toAdd = toAdd(optionalCount);
        int insertAt = insertAt(optionalCount);

        for (int i = 0; i < toAdd; i++)
            data.add(insertAt + i, "");
    }

    private int insertAt(int optionalCount) {
        return base.minCols().length + 1 + optionalCount;
    }

    private int toAdd(int optionalCount) {
        return base.optionalCols().length - optionalCount;
    }

    private int optionalCount(List data) {
        return data.size() - (2 + base.minCols().length);
    }

    private void addDefaultValuesWithComment(List data) {
        Column[] optionalCols = base.optionalCols();
        int toAdd = cols().length - data.size() - 1;

        for (int i = optionalCols.length - toAdd; i < optionalCols.length; i++)
            data.add("");
        data.add("");// comment
    }

    private boolean includesComment(List data) {
        String last = (String) data.get(data.size() - 1);
        return last.startsWith("!");
    }

    public String identify() {
        return base.identify();
    }

}
