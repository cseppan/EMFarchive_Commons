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

import java.io.File;
import java.util.Date;
import java.util.Random;

public class SpeciationProfileExporterTest extends PersistenceTestCase {

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
        SpeciationProfileImporter importer = new SpeciationProfileImporter(folder, new String[]{"gspro-speciation.txt"},
                dataset, dbServer, sqlDataTypes);
        importer.run();

        SpeciationProfileExporter exporter = new SpeciationProfileExporter(dataset, dbServer, sqlDataTypes,optimizedBatchSize);
        File file = File.createTempFile("speciatiationprofileexported", ".txt");
        exporter.export(file);
        // FIXME: compare the original file and the exported file.
        assertEquals(88, countRecords());
    }

    public void testExportVersionedChemicalSpeciationData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/speciation");
        SpeciationProfileImporter importer = new SpeciationProfileImporter(folder, new String[]{"gspro-speciation.txt"},
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer, lastModifiedDate(folder,"gspro-speciation.txt"));
        importerv.run();

        SpeciationProfileExporter exporter = new SpeciationProfileExporter(dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(version, dataset),optimizedBatchSize);
        File file = File.createTempFile("speciatiationprofileexported", ".txt");
        exporter.export(file);
        // FIXME: compare the original file and the exported file.
        assertEquals(88, countRecords());
    }

    
    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
    
    private Date lastModifiedDate(File folder, String fileName) {
        return new Date(new File(folder, fileName).lastModified());
    }
}
