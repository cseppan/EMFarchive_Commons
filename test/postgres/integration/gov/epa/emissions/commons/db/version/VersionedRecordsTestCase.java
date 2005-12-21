package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedTableFormatWithOptionalCols;

import java.sql.SQLException;

public abstract class VersionedRecordsTestCase extends PersistenceTestCase {

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
        DataModifier modifier = datasource.dataModifier();
        modifier.dropAll(versionsTable);
        
        TableDefinition def = datasource.tableDefinition();
        def.dropTable(dataTable);
    }

    protected void addRecord(Datasource datasource, String table, String[] data) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        modifier.insertRow(table, data);
    }

    private void createTable(String table, Datasource datasource) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, tableFormat().cols());
    }

    protected VersionedTableFormatWithOptionalCols tableFormat() {
        FileFormatWithOptionalCols fileFormat = new FileFormatWithOptionalCols() {
            public Column[] optionalCols() {
                return new Column[0];
            }

            public Column[] minCols() {
                Column p1 = new Column("p1", types.text());
                Column p2 = new Column("p2", types.text());

                return new Column[] { p1, p2 };
            }

            public String identify() {
                return "Record_Id";
            }

            public Column[] cols() {
                return minCols();
            }
        };
        return new VersionedTableFormatWithOptionalCols(fileFormat, types);
    }

}
