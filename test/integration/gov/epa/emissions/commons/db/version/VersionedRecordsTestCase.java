package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.VersionsColumns;
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
        dataTable = "data";

        clean();
    }

    protected void tearDown() throws Exception {
        clean();
    }

    private void clean() throws SQLException {
        DataModifier modifier = datasource.getDataModifier();
        modifier.dropAll(versionsTable);
        modifier.dropAll(dataTable);
    }

    protected void addRecord(Datasource datasource, String table, DbColumn[] cols, String[] data) throws SQLException {
        DataModifier modifier = datasource.getDataModifier();
        modifier.insertRow(table, data, cols);
    }

    protected DbColumn[] createVersionsCols() {
        return new VersionsColumns(types).get();
    }

}
