package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CEMHourSpecInventoryImExporterTest extends PersistenceTestCase {

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

    public void testImportCEMpthourData() throws Exception {
        File folder = new File("test/data/other");
        CEMHourSpecInventoryImporter importer = new CEMHourSpecInventoryImporter(folder, new String[]{"CEMpthour.txt"}, 
                dataset, dbServer, sqlDataTypes);
        importer.run();

        File exportfile = File.createTempFile("CEMpthourExported", ".txt");
        CEMHourSpecInventoryExporter exporter = new CEMHourSpecInventoryExporter(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        exporter.export(exportfile);

        List data = readData(exportfile);
        assertEquals(40, data.size());
        assertEquals(
                "ORISPL_CODE,UNITID,OP_DATE,OP_HOUR,OP_TIME,GLOAD,SLOAD,NOX_MASS,NOX_RATE,SO2_MASS,HEAT_INPUT,FLOW",
                (String) data.get(0));
        assertEquals("2161,**GT2,113,19,0,,,,,,,", (String) data.get(21));
        exportfile.delete();
    }
    
    public void testImportVersionedCEMpthourData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/other");
        CEMHourSpecInventoryImporter importer = new CEMHourSpecInventoryImporter(folder, new String[]{"CEMpthour.txt"}, 
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        File exportfile = File.createTempFile("CEMpthourExported", ".txt");
        CEMHourSpecInventoryExporter exporter = new CEMHourSpecInventoryExporter(dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(version), optimizedBatchSize);
        exporter.export(exportfile);

        List data = readData(exportfile);
        assertEquals(40, data.size());
        assertEquals(
                "ORISPL_CODE,UNITID,OP_DATE,OP_HOUR,OP_TIME,GLOAD,SLOAD,NOX_MASS,NOX_RATE,SO2_MASS,HEAT_INPUT,FLOW",
                (String) data.get(0));
        assertEquals("2161,**GT2,113,19,0,,,,,,,", (String) data.get(21));
        exportfile.delete();
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

}
