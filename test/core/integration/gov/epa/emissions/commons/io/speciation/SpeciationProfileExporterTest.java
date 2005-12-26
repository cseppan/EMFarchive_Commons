package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;
import java.util.Random;

public class SpeciationProfileExporterTest extends PersistenceTestCase {
    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;
    
    private HelpImporter delegate;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
        
        this.delegate = new HelpImporter();
        FileFormat fileFormat = new ProfileFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        String table = delegate.tableName(dataset.getName());
        FormatUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        delegate.createTable(table, datasource, formatUnit.tableFormat(), dataset.getName());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportChemicalSpeciationData() throws Exception {
        File importFile = new File("test/data/speciation","gspro-speciation.txt");
        SpeciationProfileImporter importer = new SpeciationProfileImporter(importFile, dataset, datasource, sqlDataTypes);
        importer.run();
        
        SpeciationProfileExporter exporter = new SpeciationProfileExporter(dataset, 
                datasource, sqlDataTypes);
        File file = new File("test/data/speciation","speciatiationprofileexported.txt");
        exporter.export(file);
        //FIXME: compare the original file and the exported file.
        assertEquals(88, countRecords());
        file.delete();
    }
    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
