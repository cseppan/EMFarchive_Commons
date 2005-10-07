package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataType;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;

public class TemporalProfileImporterTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataType typeMapper;

    private TemporalProfileImporter importer;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getTypeMapper();
        datasource = dbServer.getEmissionsDatasource();

        TableColumnsMetadata monthlyMeta = new TableColumnsMetadata(new MonthlyColumnsMetadata(typeMapper), typeMapper);
        createTable("Monthly", datasource, monthlyMeta);

        importer = new TemporalProfileImporter(datasource, typeMapper);
    }

    private void createTable(String table, Datasource datasource, TableColumnsMetadata colsMetadata)
            throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "Monthly");
    }

    public void testShouldReadFromFileAndLoadMonthlyPacketIntoTable() {
        File file = new File("test/data/temporal-profiles/small.txt");
        
        importer.run(file);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
//        assertEquals(10, tableReader.count(datasource.getName(), "Monthly"));
    }

}
