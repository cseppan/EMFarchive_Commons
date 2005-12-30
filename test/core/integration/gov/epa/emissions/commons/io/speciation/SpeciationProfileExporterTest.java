package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Random;

public class SpeciationProfileExporterTest extends PersistenceTestCase {
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
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportChemicalSpeciationData() throws Exception {
        File folder = new File("test/data/speciation");
        SpeciationProfileImporter importer = new SpeciationProfileImporter(folder, new String[]{"gspro-speciation.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();

        SpeciationProfileExporter exporter = new SpeciationProfileExporter(dataset, datasource, sqlDataTypes);
        File file = File.createTempFile("speciatiationprofileexported", ".txt");
        exporter.export(file);
        // FIXME: compare the original file and the exported file.
        assertEquals(88, countRecords());
    }

    public void testExportVersionedChemicalSpeciationData() throws Exception {
        File folder = new File("test/data/speciation");
        SpeciationProfileImporter importer = new SpeciationProfileImporter(folder, new String[]{"gspro-speciation.txt"},
                dataset, datasource, sqlDataTypes, new VersionedDataFormatFactory(0));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, datasource);
        importerv.run();

        SpeciationProfileExporter exporter = new SpeciationProfileExporter(dataset, datasource, sqlDataTypes,
                new VersionedDataFormatFactory(0));
        File file = File.createTempFile("speciatiationprofileexported", ".txt");
        exporter.export(file);
        // FIXME: compare the original file and the exported file.
        assertEquals(88, countRecords());
    }

    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
