package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.SummaryTable;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class ORLPointSummaryTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private Datasource emissionDatasource;

    private Datasource referenceDatasource;

    protected void setUp() throws Exception {
        super.setUp();
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        emissionDatasource = dbServer.getEmissionsDatasource();
        referenceDatasource = dbServer.getReferenceDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(emissionDatasource.getConnection());
        dbUpdate.dropTable(emissionDatasource.getName(), "test");
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");
    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        File file = new File("test/data/orl/nc", "small-point.txt");
        Importer importer = new ORLPointImporter(file, dataset, emissionDatasource, sqlDataTypes);
        importer.run();
        SummaryTable summary = new ORLPointSummary(emissionDatasource, referenceDatasource, dataset);
        summary.createSummary();
        assertEquals(10, countRecords("test"));
        assertEquals(1, countRecords("test_summary"));
    }

    private int countRecords(String tableName) {
        TableReader tableReader = new TableReader(emissionDatasource.getConnection());
        return tableReader.count(emissionDatasource.getName(), tableName);
    }

}
