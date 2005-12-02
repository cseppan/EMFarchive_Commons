package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class IDAImporterTest extends PersistenceTestCase {

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

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldImportASmallAreaFile() throws Exception {
        String source = "test/data/ida/small-area.txt";
        setInternalSource(source);
        IDANonPointImporter importer = new IDANonPointImporter(dataset, datasource, sqlDataTypes);
        importer.preCondition(null, null);
        importer.run(dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }

    public void testShouldImportASmallPointFile() throws Exception {
        String source = "test/data/ida/small-point.txt";
        setInternalSource(source);

        IDAPointImporter importer = new IDAPointImporter(dataset, datasource, sqlDataTypes);
        importer.preCondition(null, null);
        importer.run(dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }

    public void testShouldImportASmallMobileFile() throws Exception {
        String source = "test/data/ida/small-mobile.txt";
        setInternalSource(source);
        IDAMobileImporter importer = new IDAMobileImporter(dataset, datasource, sqlDataTypes);
        importer.preCondition(null, null);

        importer.run(dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }

    public void FIXME_testShouldImportASmallActivityFile() throws Exception {
        String source = "test/data/ida/small-activity.txt";
        setInternalSource(source);

        IDAActivityImporter importer = new IDAActivityImporter(dataset, datasource, sqlDataTypes);
        importer.run(dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }

    private void setInternalSource(String source) {
        File file = new File(source);

        InternalSource internalSource = new InternalSource();
        internalSource.setSource(file.getAbsolutePath());
        internalSource.setTable(dataset.getName());
        internalSource.setSourceSize(file.length());
        dataset.setInternalSources(new InternalSource[] { internalSource });
    }

}
