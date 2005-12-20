package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SurrogatesDescriptionExImporterTest extends PersistenceTestCase {
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
        
        HelpImporter delegate = new HelpImporter();
        FileFormat fileFormat = new SurrogatesDescriptionFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        FormatUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        String table = delegate.tableName(dataset.getName());
        delegate.createTable(table, datasource, formatUnit.tableFormat(), dataset.getName());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testImportCEMpthourData() throws Exception {
        File file = new File("test/data/other", "SRGDESC.txt");
        SurrogatesDescriptionImporter importer = new SurrogatesDescriptionImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        
        
        File exportfile = File.createTempFile("SRGDescExported", ".txt");
        SurrogatesDescriptionExporter exporter = new SurrogatesDescriptionExporter(dataset, datasource, sqlDataTypes);
        exporter.export(exportfile);
        
        List data = readData(file);
        assertEquals(66, data.size());
        assertEquals(
                "USA,100,\"Population\",/nas/uncch/depts/cep/emc/lran/mims/mimssp_7_2005/output/US36KM_148X112/USA_100_NOFILL.txt", 
                (String) data.get(0));
        assertEquals("USA,580,\"Food, Drug, Chemical Industrial (IND3)\",/nas/uncch/depts/cep/emc/lran/mims/mimssp_7_2005/output/US36KM_148X112/USA_580_FILL.txt", 
                (String)data.get(65));
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
