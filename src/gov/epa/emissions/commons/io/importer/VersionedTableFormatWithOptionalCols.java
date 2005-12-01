package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.NullFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VersionedTableFormatWithOptionalCols implements FileFormatWithOptionalCols, TableFormat {

    private FileFormatWithOptionalCols base;

    private Column[] cols;

    public VersionedTableFormatWithOptionalCols(FileFormatWithOptionalCols base, SqlDataTypes types) {
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

        Column recordId = new Column("Record_Id", types.autoIncrement(), new NullFormatter());
        cols.add(0, recordId);

        Column datasetId = new Column(key(), types.longType(), new LongFormatter());
        cols.add(datasetId);
        
        Column version = new Column("Version", types.longType(), new NullFormatter());
        cols.add(version);
        
        Column deleteVersions = new Column("Delete_Versions", types.text(), new NullFormatter());
        cols.add(deleteVersions);

        cols.addAll(Arrays.asList(base.cols()));

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
