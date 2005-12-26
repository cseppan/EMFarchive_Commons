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

    public void ImportSMKreportDataSemicolon() throws Exception {
        File file = new File("test/data/other", "smkreport-semicolon-state_scc.txt");
        SMKReportImporter importer = new SMKReportImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(34, countRecords());
        
        File exportfile = new File("test/data/other", "SMKreportSemicolonExported.txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource);
        exporter.export(exportfile);
        
        exportfile.delete(); 
    }
    
    public void testImportSMKreportDataPipe() throws Exception {
        File file = new File("test/data/other", "smkreport-pipe-hour_scc.txt");
        SMKReportImporter importer = new SMKReportImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(44, countRecords());
        File exportfile = new File("test/data/other", "SMKreportPipeExported.txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource);
        exporter.setDelimiter("|");
        exporter.setFormatted(true);
        exporter.export(exportfile);
        
        exportfile.delete(); 
    }
    
    public void ImportSMKreportDataQuotes() throws Exception {
        File file = new File("test/data/other", "smkreport-quotes.txt");
        SMKReportImporter importer = new SMKReportImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        File exportfile = new File("test/data/other", "SMKreportQuotesExported.txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource);
        exporter.setDelimiter("|");
        exporter.setFormatted(true);
        exporter.export(exportfile);
        
        exportfile.delete(); 
    }
    
    public void ImportSMKreportDataComma() throws Exception {
        File file = new File("test/data/other", "smkreport-comma.txt");
        SMKReportImporter importer = new SMKReportImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(67, countRecords());
        
        File exportfile = new File("test/data/other", "SMKreportCommaExported.txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource);
        exporter.export(exportfile);
        
        exportfile.delete();
    }
    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
    
    protected TableReader tableReader(Datasource datasource) {
        return dbSetup.tableReader(datasource);
    }

}
