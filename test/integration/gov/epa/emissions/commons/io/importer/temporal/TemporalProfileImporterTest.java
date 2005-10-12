package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;
import java.util.Random;

public class TemporalProfileImporterTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    private TemporalProfileImporter importer;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        TableColumnsMetadata monthlyMeta = new TableColumnsMetadata(new MonthlyColumnsMetadata(typeMapper), typeMapper);
        createTable("Monthly", datasource, monthlyMeta);

        TableColumnsMetadata weeklyMeta = new TableColumnsMetadata(new WeeklyColumnsMetadata(typeMapper), typeMapper);
        createTable("Weekly", datasource, weeklyMeta);

        TableColumnsMetadata diurnalMeta = new TableColumnsMetadata(new DiurnalColumnsMetadata(typeMapper), typeMapper);
        createTable("Diurnal_Weekday", datasource, diurnalMeta);
        createTable("Diurnal_Weekend", datasource, diurnalMeta);

        importer = new TemporalProfileImporter(datasource, typeMapper);
    }

    private void createTable(String table, Datasource datasource, TableColumnsMetadata colsMetadata)
            throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "Monthly");
        dbUpdate.dropTable(datasource.getName(), "Weekly");
        dbUpdate.dropTable(datasource.getName(), "Diurnal_Weekday");
        dbUpdate.dropTable(datasource.getName(), "Diurnal_Weekend");
    }

    public void testShouldReadFromFileAndLoadMonthlyPacketIntoTable() throws ImporterException {
        File file = new File("test/data/temporal-profiles/small.txt");

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());

        importer.run(file, dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), "Monthly"));
    }

}
