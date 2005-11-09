package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.framework.db.DbUpdate;

import java.io.File;
import java.util.Random;

public class FIXME_AllImporterSampleFilesTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String sampleFileDir = "D:/tmp11/data/orl";

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        //dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }
    
    public void testAllFiles_REMOVE(){
        assertTrue(true);
    }

    public void FIXME_testShouldImportPointFile1() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "ptinv.nti99_NC.txt");
    }

    public void FIXME_testShouldImportPointFile2() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "small.point.txt");
    }

    public void FIXME_testShouldImportNonPointFile1() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "arinv.nonpoint.nti99_NC.txt");
    }

    public void FIXME_testShouldImportNonPointFile2() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "small.nonpoint.txt");
    }

    public void FIXME_testShouldImportNonRoadFile1() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "DE_nr_cap_nei2002draft_22aug2005_orl.csv");
    }

    public void FIXME_testShouldImportNonRoadFile2() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "DE_nr_hap_nei2002draft_22aug2005_orl.csv");
    }

    public void FIXME_testShouldImportNonRoadFile3() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleFileDir, "small.nonroad.txt");
    }

    private void run(Importer importer, String dir, String fileName) throws Exception {
        try {
            importer.preCondition(new File(dir), fileName);
            importer.run(dataset);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

}
