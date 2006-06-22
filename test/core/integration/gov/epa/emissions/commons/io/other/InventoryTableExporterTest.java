package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
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
import java.util.Random;

public class InventoryTableExporterTest extends PersistenceTestCase {

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
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportChemicalSpeciationData() throws Exception {
        File folder = new File("test/data/other");
        InventoryTableImporter importer = new InventoryTableImporter(folder, new String[]{"invtable_cap.cb4.24mar2006.txt"}, 
                dataset, dbServer, sqlDataTypes);
        importer.run();
        
        InventoryTableExporter exporter = new InventoryTableExporter(dataset, 
                dbServer, sqlDataTypes,optimizedBatchSize);
        File file = File.createTempFile("inventorytableexported", ".txt");
        exporter.export(file);
        //FIXME: compare the original file and the exported file.
        assertEquals(16, countRecords());
    }
    
    public void testExportVersionedChemicalSpeciationData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/other");
        InventoryTableImporter importer = new InventoryTableImporter(folder, new String[]{"invtable_cap.cb4.24mar2006.txt"}, 
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();
        
        InventoryTableExporter exporter = new InventoryTableExporter(dataset, 
                dbServer, sqlDataTypes, new VersionedDataFormatFactory(version),optimizedBatchSize);
        File file = File.createTempFile("inventorytableexported", ".txt");
        exporter.export(file);
        //FIXME: compare the original file and the exported file.
        assertEquals(16, countRecords());
    }
    
    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
