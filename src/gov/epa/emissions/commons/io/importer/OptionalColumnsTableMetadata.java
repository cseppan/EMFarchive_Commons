package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.OptionalColumnsMetadata;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptionalColumnsTableMetadata implements OptionalColumnsMetadata {

    private OptionalColumnsMetadata base;

    private Column[] cols;

    public OptionalColumnsTableMetadata(OptionalColumnsMetadata base, SqlDataTypes types) {
        this.base = base;
        cols = createCols(types);
    }

    public String key() {
        return "Dataset_Id";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types) {
        List cols = new ArrayList();
        cols.addAll(Arrays.asList(base.cols()));

        Column datasetId = new Column(key(), types.longType(), new LongFormatter());
        cols.add(0, datasetId);

        Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
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

    public Column[] optionalCols() {
        return base.optionalCols();
    }

    public Column[] minCols() {
        return base.minCols();
    }

}
