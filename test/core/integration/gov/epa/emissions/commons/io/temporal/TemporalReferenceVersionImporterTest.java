package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class TemporalReferenceVersionImporterTest extends PersistenceTestCase {
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

        /*
         * HelpImporter delegate = new HelpImporter(); FileFormat fileFormat = new
         * TemporalReferenceFileFormat(sqlDataTypes); TableFormat tableFormat = new
         * VersionedTemporalTableFormat(fileFormat, sqlDataTypes); String table = delegate.tableName(dataset.getName());
         * FormatUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat); delegate.createTable(table, datasource,
         * formatUnit.tableFormat(), dataset.getName());
         */
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());

        dbUpdate.deleteAll(datasource.getName(), "versions");
    }

    public void testShouldImportReferenceFile() throws Exception {
        File file = new File("test/data/temporal-crossreference", "areatref.txt");
        TemporalReferenceImporter importer = new TemporalReferenceImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(34, rows);

        TemporalReferenceExporter exporter = new TemporalReferenceExporter(dataset, datasource, sqlDataTypes);
        File exportfile = new File("test/data/temporal-crossreference", "VersionedCrossRefExported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        exportfile.delete();
    }

    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
