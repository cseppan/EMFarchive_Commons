package gov.epa.emissions.commons.io.externals;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.external.ShapeFilesImporter;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

//FIXME: fix the tests - add assertions
public abstract class FIXME_ShapeFileImporterTestCase extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
        dataset = new SimpleDataset();
        dataset.setName("cnty_tn_01");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {// no op
    }

    public void testShouldImportASmallAndSimpleNonPointFile() throws Exception {
        String[] filePatterns = new String[] { "cnty_tn.*" };

        ShapeFilesImporter importer = new ShapeFilesImporter(new File("test/data/shape/cnty_tn"), filePatterns,
                dataset, datasource, sqlDataTypes);

        importer.run();
    }

}
