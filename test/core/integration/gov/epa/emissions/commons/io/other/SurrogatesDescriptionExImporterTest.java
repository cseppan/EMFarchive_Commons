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

public class SurrogatesDescriptionExImporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportImportCEMpthourData() throws Exception {
        File folder = new File("test/data/other");
        SurrogatesDescriptionImporter importer = new SurrogatesDescriptionImporter(folder, new String[]{"SRGDESC.txt"}, 
                dataset, dbServer, sqlDataTypes);
        importer.run();

        File exportfile = File.createTempFile("SRGDescExported", ".txt");
        SurrogatesDescriptionExporter exporter = new SurrogatesDescriptionExporter(dataset, dbServer, sqlDataTypes);
        exporter.export(exportfile);

        List data = readData(exportfile);
        assertEquals(66, data.size());
        assertEquals(
                "USA;100;\"Population\";/nas/uncch/depts/cep/emc/lran/mims/mimssp_7_2005/output/US36KM_148X112/USA_100_NOFILL.txt",
                (String) data.get(0));
        assertEquals(
                "USA;580;\"Food, Drug, Chemical Industrial (IND3)\";/nas/uncch/depts/cep/emc/lran/mims/mimssp_7_2005/output/US36KM_148X112/USA_580_FILL.txt",
                (String) data.get(65));
        exportfile.delete();
    }
    
    public void testExportImportVersionedCEMpthourData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/other");
        SurrogatesDescriptionImporter importer = new SurrogatesDescriptionImporter(folder, new String[]{"SRGDESC.txt"}, 
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        File exportfile = File.createTempFile("SRGDescExported", ".txt");
        SurrogatesDescriptionExporter exporter = new SurrogatesDescriptionExporter(dataset, dbServer, 
                sqlDataTypes, new VersionedDataFormatFactory(version));
        exporter.export(exportfile);

        List data = readData(exportfile);
        assertEquals(66, data.size());
        assertEquals(
                "USA;100;\"Population\";/nas/uncch/depts/cep/emc/lran/mims/mimssp_7_2005/output/US36KM_148X112/USA_100_NOFILL.txt",
                (String) data.get(0));
        assertEquals(
                "USA;580;\"Food, Drug, Chemical Industrial (IND3)\";/nas/uncch/depts/cep/emc/lran/mims/mimssp_7_2005/output/US36KM_148X112/USA_580_FILL.txt",
                (String) data.get(65));
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
