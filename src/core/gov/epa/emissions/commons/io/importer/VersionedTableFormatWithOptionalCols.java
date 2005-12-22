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
        Column recordId = recordID(types);
        Column datasetId = new Column("Dataset_Id", types.longType(), new LongFormatter(), "NOT NULL");
        Column version = new Column("Version", types.longType(), new NullFormatter(), "NULL DEFAULT 0");
        Column deleteVersions = new Column("Delete_Versions", types.text(), new NullFormatter(), "DEFAULT ''::text");

        return new Column[] { recordId, datasetId, version, deleteVersions };
    }

    private Column recordID(SqlDataTypes types) {
        String key = key();
        String keyType = types.autoIncrement() + ", Primary Key(" + key + ")";
        Column recordId = new Column(key, keyType, new NullFormatter());
        return recordId;
    }

    private int numVersionCols() {
        return 4;
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

    public void fillDefaults(List data, long datasetId) {
        // TODO Auto-generated method stub

    }

}
