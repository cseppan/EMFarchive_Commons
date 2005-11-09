package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.framework.db.DbUpdate;

import java.io.File;
import java.util.Random;

public class IDAHeaderTagsTest extends DbTestCase {

    private SqlDataTypes sqlDataTypes;

    private Datasource datasource;

    private SimpleDataset dataset;

    protected void setUp() throws Exception {
        super.setUp();
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());
    }

    private void dropTable() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldIdentifyAllRequiredTags() throws Exception {
        String source = "test/data/ida/small-area.txt";
        InternalSource internalSource = internalSource(source);
        dataset.setInternalSources(new InternalSource[] { internalSource });
        IDAImporter importer = new IDAImporter(dataset, datasource, sqlDataTypes);
        importer.preImport(new IDANonPointFileFormat(sqlDataTypes));
        importer.run();
        dropTable();
    }



    public void testShouldIdentifyNoIDATag() throws Exception {
        String source = "test/data/ida/noIDATags.txt";
        InternalSource internalSource = internalSource(source);
        dataset.setInternalSources(new InternalSource[] { internalSource });

        IDAImporter importer = new IDAImporter(dataset, datasource, sqlDataTypes);
        try {
            importer.preImport(new IDANonPointFileFormat(sqlDataTypes));
            importer.run();
            assertTrue(false);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            assertTrue(e.getMessage().startsWith("The tag - 'IDA' is mandatory"));
        }
    }

    private InternalSource internalSource(String source) {
        File file = new File(source);

        InternalSource internalSource = new InternalSource();
        internalSource.setSource(file.getAbsolutePath());
        internalSource.setTable(dataset.getName());
        internalSource.setSourceSize(file.length());
        return internalSource;
    }

}
