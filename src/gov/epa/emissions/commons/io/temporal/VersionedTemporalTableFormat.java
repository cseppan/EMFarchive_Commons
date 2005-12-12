package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.VersionedRecord;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.LongFormatter;
import gov.epa.emissions.commons.io.NullFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VersionedTemporalTableFormat implements FileFormat, TableFormat {
    private FileFormat base;

    private Column[] cols;

    public VersionedTemporalTableFormat(FileFormat base, SqlDataTypes types) {
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
        // TODO: these constraints are Postgres-specific. Generic constraints ?
        Column recordId = new Column(key(), types.autoIncrement(), new NullFormatter(), "NOT NULL");
        Column datasetId = new Column("Dataset_Id", types.longType(), new LongFormatter(), "NOT NULL");
        Column version = new Column("Version", types.longType(), new NullFormatter(), "NULL DEFAULT 0");
        Column deleteVersions = new Column("Delete_Versions", types.text(), new NullFormatter(), "DEFAULT ''::text");

        return new Column[] { recordId, datasetId, version, deleteVersions };
    }

    public void fillDefaults(List data, long datasetId) {
        addVersionData(data, datasetId, 0);
        addComments(data);
    }

    public List fill(VersionedRecord record, int version) {
        List data = new ArrayList();

        addVersionData(data, record.getDatasetId(), version);
        data.addAll(record.tokens());
        addComments(data);

        return data;
    }

    private void addComments(List data) {
        String last = (String) data.get(data.size() - 1);
        if (last != null && !last.startsWith("!"))
            data.add(data.size(), "");// empty comment
    }

    private void addVersionData(List data, long datasetId, int version) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version + "");// version
        data.add(3, "");// delete versions
    }

    public String identify() {
        return base.identify();
    }
}
