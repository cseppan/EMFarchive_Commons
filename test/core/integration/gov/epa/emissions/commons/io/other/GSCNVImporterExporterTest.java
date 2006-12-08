package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GSCNVImporterExporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    private Integer optimizedBatchSize;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        optimizedBatchSize = new Integer(1000);

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));
        dataset.setDatasetType(new DatasetType("dsType"));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), "GSCNV");
    }

    public void testImportVersionedGSCNVFile() throws ImporterException {
        File folder = new File("test/data/other");
        Importer importer = new GSCNVImporter(folder, new String[] { "gscnv_cb05_notoxics_cmaq_29aug2006.out.txt" },
                dataset, dbServer, sqlDataTypes);
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer, lastModifiedDate(folder,"gscnv_cb05_notoxics_cmaq_29aug2006.out.txt"));
        importerv.run();

        assertEquals(1231, countRecords(dbServer, "GSCNV"));
    }

    public void testExportVersionedGSCNVFile() throws Exception {
        File folder = new File("test/data/other");
        Importer importer = new GSCNVImporter(folder, new String[] { "gscnv_cb05_notoxics_cmaq_29aug2006.out.txt" },
                dataset, dbServer, sqlDataTypes);
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer, lastModifiedDate(folder,"gscnv_cb05_notoxics_cmaq_29aug2006.out.txt"));
        importerv.run();

        assertEquals(1231, countRecords(dbServer, "GSCNV"));

        Exporter exporter = new GSCNVExporter(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        File file = doExport(exporter);
        List records = readData(file);
        assertEquals(1231, records.size());
        assertEquals(1231, exporter.getExportedLinesCount());
    }

    private File doExport(Exporter exporter) throws Exception {
        File file = File.createTempFile("exported", ".orl");
        file.deleteOnExit();

        exporter.export(file);

        return file;
    }

    private List readData(File file) throws IOException {
        List data = new ArrayList();

        BufferedReader r = new BufferedReader(new FileReader(file));
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            if (isNotEmpty(line) && !isComment(line))
                data.add(line);
        }

        return data;
    }
    
    private boolean isNotEmpty(String line) {
        return line.length() != 0;
    }
    
    private boolean isComment(String line) {
        return line.startsWith("#");
    }
    
    private Date lastModifiedDate(File folder, String fileName) {
        return new Date(new File(folder, fileName).lastModified());
    }

}
