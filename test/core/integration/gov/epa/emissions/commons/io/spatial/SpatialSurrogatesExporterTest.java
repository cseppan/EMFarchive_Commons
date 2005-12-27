package gov.epa.emissions.commons.io.spatial;

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
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;
import java.util.Random;

public class SpatialSurrogatesExporterTest extends PersistenceTestCase {
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

        FileFormat fileFormat = new SpatialSurrogatesFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        
        DataTable dataTable = new DataTable(dataset, datasource);
        FormatUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        dataTable.create(formatUnit.tableFormat());
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportSpetailSurrogatesData() throws Exception {
        File file = new File("test/data/spatial", "abmgpro.txt");
        SpatialSurrogatesImporter importer = new SpatialSurrogatesImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        SpatialSurrogatesExporter exporter = new SpatialSurrogatesExporter(dataset, datasource, sqlDataTypes);
        File exportfile = new File("test/data/spatial", "SpetialSurrogatesExported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        exportfile.delete();
        assertEquals(43, countRecords());
    }

    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
