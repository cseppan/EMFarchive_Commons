package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.ida.IDAMobileImporter;
import gov.epa.emissions.commons.io.ida.IDANonPointImporter;
import gov.epa.emissions.commons.io.ida.IDAPointImporter;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.orl.ORLNonPointImporter;
import gov.epa.emissions.commons.io.orl.ORLNonRoadImporter;
import gov.epa.emissions.commons.io.orl.ORLPointImporter;
import gov.epa.emissions.framework.db.DbUpdate;

import java.io.File;
import java.util.Random;

public class AllImporterSampleFilesTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String sampleFileDir = "D:/tmp11/data/";

    private String sampleORLFileDir;

    private String sampleIDAFileDir;
    
    private String sampleNIFFileDir;

    protected void setUp() throws Exception {
        super.setUp();
        sampleORLFileDir = sampleFileDir + "orl/";
        sampleIDAFileDir = sampleFileDir + "ida/";
        sampleNIFFileDir = sampleFileDir + "nif/";

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
    
    public void testREMOVE(){
        assertTrue(true);
    }
    
    //FIXME: The bug with WhiteSpaceTokenizer has to be fixed
    public void FIXME_testShouldImportORLPointFile1() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "ptinv.nti99_NC.txt");
    }

    public void itestShouldImportORLPointFile2() throws Exception {
        Importer importer = new ORLPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "small.point.txt");
    }

    public void itestShouldImportORLNonPointFile1() throws Exception {
        Importer importer = new ORLNonPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "arinv.nonpoint.nti99_NC.txt");
    }

    public void itestShouldImportORLNonPointFile2() throws Exception {
        Importer importer = new ORLNonPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "small.nonpoint.txt");
    }

    public void itestShouldImportORLNonRoadFile1() throws Exception {
        Importer importer = new ORLNonRoadImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "DE_nr_cap_nei2002draft_22aug2005_orl.csv");
    }

    public void itestShouldImportORLNonRoadFile2() throws Exception {
        Importer importer = new ORLNonRoadImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "DE_nr_hap_nei2002draft_22aug2005_orl.csv");
    }

    public void itestShouldImportORLNonRoadFile3() throws Exception {
        Importer importer = new ORLNonRoadImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleORLFileDir, "small.nonroad.txt");
    }

    public void itestShouldImportIDANonPointFile1() throws Exception {
        String fileName = "area_FugitiveDust.txt";
        setIDAInternalSource(fileName);
        Importer importer = new IDANonPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDANonPointFile2() throws Exception {
        String fileName = "arinv.stationary.nei96_NC.ida.txt";
        setIDAInternalSource(fileName);
        Importer importer = new IDANonPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDANonPointFile3() throws Exception {
        String fileName = "arinv_nr_2001_small.emis";
        setIDAInternalSource(fileName);
        Importer importer = new IDANonPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDANonPointFile4() throws Exception {
        String fileName = "nr_01v3-OTAQblend_040309_cmv.ida";
        setIDAInternalSource(fileName);
        Importer importer = new IDANonPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDAPointFile1() throws Exception {
        String fileName = "Point_PMFugitive.txt";
        setIDAInternalSource(fileName);
        Importer importer = new IDAPointImporter(dataset, datasource, sqlDataTypes);
        setIDAInternalSource(fileName);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDAPointFile2() throws Exception {
        String fileName = "pt_fug_01v3_031211.emis.txt";
        setIDAInternalSource(fileName);
        Importer importer = new IDAPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDAPointFile3() throws Exception {
        String fileName = "ptinv.nei96_NC.ida.txt";
        setIDAInternalSource(fileName);
        Importer importer = new IDAPointImporter(dataset, datasource, sqlDataTypes);
        run(importer, sampleIDAFileDir, fileName);
    }

    public void itestShouldImportIDAMobile() throws Exception {
        String fileName = "mbinv.nei96.ida_emis.txt";
        setIDAInternalSource(fileName);
        Importer importer = new IDAMobileImporter(dataset, datasource, sqlDataTypes);

        run(importer, sampleIDAFileDir, fileName);
    }
    

    private void setIDAInternalSource(String fileName) {
        File file = new File(sampleIDAFileDir, fileName);
        InternalSource iSource = new InternalSource();
        iSource.setSource(file.getAbsolutePath());
        iSource.setTable(dataset.getName());
        iSource.setSourceSize(file.length());
        dataset.setInternalSources(new InternalSource[] { iSource });
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
