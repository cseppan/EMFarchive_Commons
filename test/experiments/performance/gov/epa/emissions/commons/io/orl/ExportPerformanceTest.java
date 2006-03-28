package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.PerformanceTestCase;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;

import java.io.File;
import java.util.Random;

public abstract class ExportPerformanceTest extends PerformanceTestCase {
    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    private VersionedDataFormatFactory formatFactory;

    private Version version;

    public ExportPerformanceTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));

        version = new Version();
        version.setVersion(0);
        formatFactory = new VersionedDataFormatFactory(version);
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    protected void doImport(File importFile) throws Exception {
        DataFormatFactory formatFactory = new VersionedDataFormatFactory(version);
        Importer importer = new ORLOnRoadImporter(importFile.getParentFile(), new String[] { importFile.getName() },
                dataset, dbServer, sqlDataTypes, formatFactory);
        importer.run();
        System.out.println("Import completed.");
    }

    protected void doExport() throws Exception {
        Exporter exporter = new ORLOnRoadExporter(dataset, dbServer, sqlDataTypes, formatFactory);
        File file = File.createTempFile("exported", ".orl");
        file.deleteOnExit();

        exporter.export(file);
    }

}
