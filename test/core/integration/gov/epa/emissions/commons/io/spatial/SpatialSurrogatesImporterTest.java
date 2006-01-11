package gov.epa.emissions.commons.io.spatial;

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

public class SpatialSurrogatesImporterTest extends PersistenceTestCase {

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
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testImportSpetialSurrogatesData() throws Exception {
        File folder = new File("test/data/spatial");
        SpatialSurrogatesImporter importer = new SpatialSurrogatesImporter(folder, new String[]{"abmgpro.txt"}, dataset, dbServer, sqlDataTypes);
        importer.run();

        assertEquals(43, countRecords());
    }

    public void testImportVersionedSpetialSurrogatesData() throws Exception {
        File folder = new File("test/data/spatial");
        SpatialSurrogatesImporter importer = new SpatialSurrogatesImporter(folder, new String[]{"abmgpro.txt"}, 
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(0));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        assertEquals(43, countRecords());
    }

    
    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
