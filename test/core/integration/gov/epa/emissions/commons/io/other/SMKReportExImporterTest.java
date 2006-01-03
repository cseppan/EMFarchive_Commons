package gov.epa.emissions.commons.io.other;

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

public class SMKReportExImporterTest extends PersistenceTestCase {
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
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testImportSMKreportDataSemicolon() throws Exception {
        File folder = new File("test/data/other");
        SMKReportImporter importer = new SMKReportImporter(folder, new String[]{"smkreport-semicolon-state_scc.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(34, countRecords());
        
        File exportfile = File.createTempFile("SMKreportSemicolonExported", ".txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource, sqlDataTypes);
        exporter.export(exportfile);
        //FIXME: put assert statement
    }
    
    public void testImportSMKreportDataPipe() throws Exception {
        File folder = new File("test/data/other");
        SMKReportImporter importer = new SMKReportImporter(folder, new String[]{"smkreport-pipe-hour_scc.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(44, countRecords());
        
        File exportfile = File.createTempFile("SMKreportPipeExported", ".txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource, sqlDataTypes);
        exporter.export(exportfile);
        exporter.setDelimiter("|");
        exporter.setFormatted(true);
        exporter.export(exportfile);
    }
    
    public void testImportSMKreportDataQuotes() throws Exception {
        File folder = new File("test/data/other");
        SMKReportImporter importer = new SMKReportImporter(folder, new String[]{"smkreport-quotes.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();

        File exportfile = File.createTempFile("SMKreportQuotesExported", ".txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource, sqlDataTypes);
        exporter.export(exportfile);
        exporter.setDelimiter("|");
        exporter.setFormatted(true);
        exporter.export(exportfile);
    }
    
    public void testImportSMKreportDataComma() throws Exception {
        File folder = new File("test/data/other");
        SMKReportImporter importer = new SMKReportImporter(folder, new String[]{"smkreport-comma.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(67, countRecords());
        
        File exportfile = File.createTempFile("SMKreportCommaExported", ".txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource, sqlDataTypes);
        exporter.export(exportfile);
        exporter.export(exportfile);
    }
    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
    
    protected TableReader tableReader(Datasource datasource) {
        return dbSetup.tableReader(datasource);
    }

}
