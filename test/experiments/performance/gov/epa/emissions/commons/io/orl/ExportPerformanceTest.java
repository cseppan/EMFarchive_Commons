package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.PerformanceTestCase;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.Exporter;
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
        dataset.setId(Math.abs(new Random().nextInt()));

        version = new Version();
        version.setVersion(0);
        formatFactory = new VersionedDataFormatFactory(version);
    }

    protected void doExport(String datasetName) throws Exception {
        dataset.setName(datasetName);
        InternalSource table = new InternalSource();
        table.setTable(datasetName);
        dataset.addInternalSource(table);
        Integer optimizedBatchSize = new Integer(10000);
        Exporter exporter = new ORLOnRoadExporter(dataset, dbServer, sqlDataTypes, formatFactory, optimizedBatchSize);
        File file = File.createTempFile("exported", ".orl");
        file.deleteOnExit();

        exporter.export(file);
    }

}
