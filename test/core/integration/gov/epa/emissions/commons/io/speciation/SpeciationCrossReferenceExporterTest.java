package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class SpeciationCrossReferenceExporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    private Integer optimizedBatchSize;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        
        optimizedBatchSize = new Integer(10000);

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));
        dataset.setDatasetType(new DatasetType("dsType"));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportChemicalSpeciationData() throws Exception {
        File folder = new File("test/data/speciation");
        SpeciationCrossReferenceImporter importer = new SpeciationCrossReferenceImporter(folder,
                new String[] { "gsref-point.txt" }, dataset, dbServer, sqlDataTypes);
        importer.run();

        SpeciationCrossReferenceExporter exporter = new SpeciationCrossReferenceExporter(dataset, dbServer,
                sqlDataTypes,optimizedBatchSize);
        File exportfile = File.createTempFile("SpeciatiationCrossRefExported", ".txt");
        exporter.export(exportfile);

        List data = readData(exportfile);
        assertEquals(153, countRecords());
        assertEquals("0;\"0000\";\"EXH__CO\";;;;\"\";\"\";\"\";\"\"! exhaust for MOBILE5", data.get(2));
        assertEquals("2850000010;\"99999\";\"PM2_5\";;;;\"\";\"\";\"\";\"\"! heavy duty diesel trucks-specific", data.get(153));
        assertEquals(153, exporter.getExportedLinesCount());
    }

    public void testExportVersionedChemicalSpeciationData() throws Exception {
        Version version = version();
        
        File folder = new File("test/data/speciation");
        SpeciationCrossReferenceImporter importer = new SpeciationCrossReferenceImporter(folder,
                new String[] { "gsref-point.txt" }, dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(
                        version, dataset));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer, lastModifiedDate(folder,"gsref-point.txt"));
        importerv.run();

        SpeciationCrossReferenceExporter exporter = new SpeciationCrossReferenceExporter(dataset, dbServer,
                sqlDataTypes, new VersionedDataFormatFactory(version, dataset),optimizedBatchSize);
        File exportfile = File.createTempFile("SpeciatiationCrossRefExported", ".txt");
        exporter.export(exportfile);

        List data = readData(exportfile);
        assertEquals(153, countRecords());
        
        assertEquals("0;\"0000\";\"EXH__CO\";;;;\"\";\"\";\"\";\"\"! exhaust for MOBILE5", data.get(2));
        assertEquals("2850000010;\"99999\";\"PM2_5\";;;;\"\";\"\";\"\";\"\"! heavy duty diesel trucks-specific", data.get(153));
    }

    private Version version() {
        Version version = new Version();
        version.setVersion(0);
        version.setDatasetId(dataset.getId());
        return version;
    }

    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
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
