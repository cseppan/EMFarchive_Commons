package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class MobileTemporalReferenceImporterTest extends PersistenceTestCase {
    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        MobileTemporalReferenceFileFormat base = new MobileTemporalReferenceFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(base, sqlDataTypes);
        createTable("MOBILE_SOURCE", datasource, tableFormat);
    }

    protected void tearDown() throws Exception {
        dropTable("MOBILE_SOURCE", datasource);
    }

    public void testShouldImportAFileWithVariableCols() throws Exception {
        File file = new File("test/data/temporal-crossreference", "areatref.txt");
        MobileTemporalReferenceImporter importer = new MobileTemporalReferenceImporter(file, dataset, datasource,
                sqlDataTypes);
        importer.run();

        assertEquals(34, countRecords("MOBILE_SOURCE"));
    }

    private int countRecords(String table) {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), table);
    }
}
