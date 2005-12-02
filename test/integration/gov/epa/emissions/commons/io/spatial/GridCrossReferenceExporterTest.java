package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class GridCrossReferenceExporterTest extends PersistenceTestCase {
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
        dataset.setDatasetid(new Random().nextLong());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportGridCrossRefData() throws Exception {
        GridCrossReferenceImporter importer = new GridCrossReferenceImporter(datasource, sqlDataTypes);
        importer.preCondition(new File("test/data/spatial"), "amgref.txt");
        importer.run(dataset);
        
        GridCrossReferenceExporter exporter = new GridCrossReferenceExporter(dataset, 
                datasource, new GridCrossRefFileFormat(sqlDataTypes));
        File file = new File("test/data/spatial","GridCrossRefExported.txt");
        exporter.export(file);
        //FIXME: compare the original file and the exported file.
        assertEquals(22, countRecords());
        file.delete();
    }
    
    private int countRecords() {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
