package gov.epa.emissions.framework.db;

import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;

public class VersionDataColumns {

    private SqlDataTypes types;

    public VersionDataColumns(SqlDataTypes types) {
        this.types = types;
    }

    public DbColumn[] get() {
        DbColumn recordId = new Column("record_id", types.intType(), null);
        DbColumn datasetId = new Column("dataset_id", types.intType(), null);
        DbColumn version = new Column("version", types.intType(), null);
        DbColumn deleteVersion = new Column("  delete_version", types.stringType(255), null);
        DbColumn param1 = new Column("param1", types.stringType(255), null);
        DbColumn param2 = new Column("param2", types.stringType(255), null);

        return new DbColumn[] { recordId, datasetId, version, deleteVersion, param1, param2 };
    }

}
