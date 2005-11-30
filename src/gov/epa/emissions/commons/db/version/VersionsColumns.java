package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;

public class VersionsColumns {

    private SqlDataTypes types;

    public VersionsColumns(SqlDataTypes types) {
        this.types = types;
    }

    public DbColumn[] get() {
        DbColumn datasetId = new Column("dataset_id", types.intType(), null);
        DbColumn version = new Column("version", types.intType(), null);
        DbColumn parentVersions = new Column("path", types.stringType(255), null);

        return new DbColumn[] { datasetId, version, parentVersions };
    }

}
