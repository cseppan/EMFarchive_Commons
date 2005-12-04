package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.db.version.VersionsColumns;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.sql.SQLException;

public class VersionedRecordsTestCase extends PersistenceTestCase {

    protected Datasource datasource;

    protected SqlDataTypes types;

    protected String versionsTable;

    protected String dataTable;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getSqlDataTypes();

        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";
        dataTable = "versioned_data";

        createTable(dataTable, datasource);
    }

    protected void tearDown() throws Exception {
        clean();
    }

    private void clean() throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        modifier.dropAll(versionsTable);

        TableDefinition def = datasource.tableDefinition();
        def.dropTable(dataTable);
    }

    protected void addRecord(Datasource datasource, String table, DbColumn[] cols, String[] data) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        modifier.insertRow(table, data, cols);
    }

    protected DbColumn[] createVersionsCols() {
        return new VersionsColumns(types).get();
    }

    private void createTable(String table, Datasource datasource) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();

        DbColumn recordId = new Column("record_id", types.autoIncrement(), "NOT NULL");
        DbColumn datasetId = new Column("dataset_id", types.intType(), "NOT NULL");
        DbColumn version = new Column("version", types.intType(), "NULL DEFAULT 0");
        DbColumn deleteVersions = new Column("delete_versions", types.text(), "DEFAULT ''::text");
        DbColumn param1 = new Column("param1", types.text());
        DbColumn param2 = new Column("param2", types.text());

        DbColumn[] cols = new DbColumn[] { recordId, datasetId, version, deleteVersions, param1, param2 };

        tableDefinition.createTable(table, cols);
    }

}
