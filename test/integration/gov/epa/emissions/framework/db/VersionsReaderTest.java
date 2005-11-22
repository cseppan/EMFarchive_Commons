package gov.epa.emissions.framework.db;

import java.sql.SQLException;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

public class VersionsReaderTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes types;

    private String versionsTable;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";
        setupData(datasource, versionsTable);
    }

    private void setupData(Datasource datasource, String table) throws SQLException {
        DbColumn[] cols = createVersionsCols();

        String[] data = { "1", "0", "" };

        DataModifier modifier = datasource.getDataModifier();
        modifier.insertRow(table, data, cols);
    }

    private DbColumn[] createVersionsCols() {
        DbColumn datasetId = new Column("dataset_id", types.intType(), null);
        DbColumn version = new Column("version", types.intType(), null);
        DbColumn parentVersions = new Column("parent_versions", types.stringType(255), null);
        DbColumn[] cols = { datasetId, version, parentVersions };

        return cols;
    }

    protected void tearDown() throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        modifier.dropAll(versionsTable);
    }

    public void testFetchVersionZero() {
        VersionsReader reader = new VersionsReader(datasource);

        int[] versions = reader.fetch(1, 0);

        assertEquals(1, versions.length);
        assertEquals(0, versions[0]);
    }
}
