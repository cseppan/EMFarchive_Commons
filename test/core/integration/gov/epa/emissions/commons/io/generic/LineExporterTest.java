package gov.epa.emissions.commons.io.generic;

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
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class LineExporterTest extends PersistenceTestCase {
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

        FileFormat fileFormat = new LineFileFormat(sqlDataTypes);
        TableFormat tableFormat = new LineTableFormat(fileFormat, sqlDataTypes);
        
        DataTable dataTable = new DataTable(dataset);
        String table = dataTable.tableName();
        FormatUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        dataTable.create(table, datasource, formatUnit.tableFormat());
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportSmallLineFile() throws Exception {
        File importFile = new File("test/data/orl/nc", "small-point.txt");
        LineImporter importer = new LineImporter(importFile, dataset, datasource, sqlDataTypes);
        importer.run();

        LineExporter exporter = new LineExporter(dataset, datasource, new LineFileFormat(sqlDataTypes));
        File file = new File("test/data/orl/nc", "lineexporter.txt");
        exporter.export(file);
        // FIXME: run the comparison tool, look at other exporter test
        assertEquals(22, countRecords());
        file.delete();
    }

    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
