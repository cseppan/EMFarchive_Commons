package gov.epa.emissions.commons.io.speciation;

import java.io.File;
import java.util.Random;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

public class SpeciationProfileExporterTest extends PersistenceTestCase {
    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportChemicalSpeciationData() throws Exception {
        SpeciationProfileImporter importer = new SpeciationProfileImporter(datasource, sqlDataTypes, "Chem Spec");
        importer.preCondition(new File("test/data/speciation"), "gspro-speciation.txt");
        importer.run(dataset);
        
        SpeciationProfileExporter exporter = new SpeciationProfileExporter(dataset, 
                datasource, new ProfileFileFormat("Chem Speciation Profile", sqlDataTypes));
        exporter.export(new File("C:\\awork\\exported\\speciatiationprofileexported.txt"));

        assertEquals(88, countRecords());
    }
    
    private int countRecords() {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
