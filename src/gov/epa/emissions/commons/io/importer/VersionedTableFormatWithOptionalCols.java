package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.NullFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VersionedTableFormatWithOptionalCols implements TableFormatWithOptionalCols {

    private FileFormatWithOptionalCols base;

    private Column[] cols;

    public VersionedTableFormatWithOptionalCols(FileFormatWithOptionalCols base, SqlDataTypes types) {
        this.base = base;
        cols = createCols(types);
    }

    public String key() {
        return "Record_Id";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types) {
        List cols = new ArrayList();

        cols.addAll(Arrays.asList(versionCols(types)));
        // sandwich data b/w version cols and Comments
        cols.addAll(Arrays.asList(base.cols()));

        Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
        cols.add(inlineComments);

        return (Column[]) cols.toArray(new Column[0]);
    }

    private Column[] versionCols(SqlDataTypes types) {
        Column recordId = new Column(key(), types.autoIncrement(), new NullFormatter());
        Column datasetId = new Column("Dataset_Id", types.longType(), new LongFormatter());
        Column version = new Column("Version", types.longType(), new NullFormatter());
        Column deleteVersions = new Column("Delete_Versions", types.text(), new NullFormatter());

        return new Column[] { recordId, datasetId, version, deleteVersions };
    }

    // FIXME: rework this mess
    public void fill(List data, long datasetId) {
        addVersionData(data, datasetId);
        addComments(data);

        addDefaultValuesForOptionalCols(data);
    }

    private void addComments(List data) {
        String last = (String) data.get(data.size() - 1);
        if (!last.startsWith("!"))
            data.add(data.size(), "");// empty comment
    }

    private void addVersionData(List data, long datasetId) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, "0");// version
        data.add(3, "");// delete versions
    }

    private void addDefaultValuesForOptionalCols(List data) {
        int optionalCount = optionalCount(data);
        int toAdd = toAdd(optionalCount);
        int insertAt = insertAt(optionalCount);

        for (int i = 0; i < toAdd; i++)
            data.add(insertAt + i, "");
    }

    private int insertAt(int optionalCount) {
        return numVersionCols() + base.minCols().length + optionalCount;
    }

    private int numVersionCols() {
        return 4;
    }

    private int toAdd(int optionalCount) {
        return base.optionalCols().length - optionalCount;
    }

    private int optionalCount(List data) {
        return data.size() - numFixedCols();
    }

    private int numFixedCols() {
        return numVersionCols() + base.minCols().length + 1;// 1 - comments
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
