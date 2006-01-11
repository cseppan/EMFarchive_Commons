package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FixedColsTableFormat;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public abstract class FIXME_PointTemporalReferenceImporterTestCase extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        PointTemporalReferenceFileFormat base = new PointTemporalReferenceFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(base, sqlDataTypes);
        createTable("POINT_SOURCE", dbServer.getEmissionsDatasource(), tableFormat);
    }

    protected void doTearDown() throws Exception {
        dropTable("POINT_SOURCE", dbServer.getEmissionsDatasource());
    }

    public void testShouldImportAFileWithVariableCols() throws Exception {
        File file = new File("test/data/temporal-crossreference", "point-source-VARIABLE-COLS.txt");
        FIX_THE_TEST_PointTemporalReferenceImporter importer = new FIX_THE_TEST_PointTemporalReferenceImporter(file, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        assertEquals(20, countRecords("POINT_SOURCE"));
    }

    private int countRecords(String table) {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), table);
    }

    public void testShouldSetFullLineCommentsAndDescCommentsAsDatasetDescriptionOnImport() throws Exception {
        File file = new File("test/data/temporal-crossreference", "point-source.txt");
        FIX_THE_TEST_PointTemporalReferenceImporter importer = new FIX_THE_TEST_PointTemporalReferenceImporter(file, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        // assert
        String expected = "# comment1\n#comment 2  \n#comment 3\n";
        assertEquals(expected, dataset.getDescription());
    }

}
