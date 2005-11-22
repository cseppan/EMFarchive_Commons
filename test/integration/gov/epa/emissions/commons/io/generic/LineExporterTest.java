package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.generic.LineFileFormat;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.Random;

public class LineExporterTest extends PersistenceTestCase {
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
    
    

    public void testExportSmallLineFile() throws Exception {
        LineImporter importer = new LineImporter(datasource, sqlDataTypes);
        importer.preCondition(new File("test/data/orl/nc"), "small-point.txt");
        importer.run(dataset);
        
        LineExporter exporter = new LineExporter (dataset, datasource, new LineFileFormat(sqlDataTypes));
        File file = new File("test/data/orl/nc","lineexporter.txt");
        exporter.export(file);
        //FIXME: run the comparison tool, look at other exporter test
        assertEquals(22, countRecords());
        file.delete();
    }
    
    private int countRecords() {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
