package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    protected void tearDown() throws Exception {
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
        
        List data = readData(file);
        //assertEquals(34, data.size());
        //assertEquals(
        //        "ORISPL_CODE,UNITID,OP_DATE,OP_HOUR,OP_TIME,GLOAD,SLOAD,NOX_MASS,NOX_RATE,SO2_MASS,HEAT_INPUT,FLOW", 
        //        (String) data.get(0));
        //assertEquals("2161,**GT2,000113,19,0,-9,-9,-9,-9,-9,-9,-9", (String)data.get(21));
        //exportfile.delete(); 
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
        
        List data = readData(file);
        //assertEquals(40, data.size());
        //assertEquals(
        //        "ORISPL_CODE,UNITID,OP_DATE,OP_HOUR,OP_TIME,GLOAD,SLOAD,NOX_MASS,NOX_RATE,SO2_MASS,HEAT_INPUT,FLOW", 
        //        (String) data.get(0));
        //assertEquals("2161,**GT2,000113,19,0,-9,-9,-9,-9,-9,-9,-9", (String)data.get(21));
        //exportfile.delete(); */
    }
    
    public void ImportSMKreportDataQuotes() throws Exception {
        File file = new File("test/data/other", "smkreport-quotes.txt");
        SMKReportImporter importer = new SMKReportImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        //assertEquals(44, countRecords());
        File exportfile = new File("test/data/other", "SMKreportQuotesExported.txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource);
        exporter.setDelimiter("|");
        exporter.setFormatted(true);
        exporter.export(exportfile);
        
        //List data = readData(file);
        //assertEquals(40, data.size());
        //assertEquals(
        //        "ORISPL_CODE,UNITID,OP_DATE,OP_HOUR,OP_TIME,GLOAD,SLOAD,NOX_MASS,NOX_RATE,SO2_MASS,HEAT_INPUT,FLOW", 
        //        (String) data.get(0));
        //assertEquals("2161,**GT2,000113,19,0,-9,-9,-9,-9,-9,-9,-9", (String)data.get(21));
        //exportfile.delete(); */
    }
    
    public void ImportSMKreportDataComma() throws Exception {
        File file = new File("test/data/other", "smkreport-comma.txt");
        SMKReportImporter importer = new SMKReportImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(67, countRecords());
        
        File exportfile = new File("test/data/other", "SMKreportCommaExported.txt");
        SMKReportExporter exporter = new SMKReportExporter(dataset, datasource);
        exporter.export(exportfile);
        
        List data = readData(file);
        //assertEquals(40, data.size());
        //assertEquals(
        //        "ORISPL_CODE,UNITID,OP_DATE,OP_HOUR,OP_TIME,GLOAD,SLOAD,NOX_MASS,NOX_RATE,SO2_MASS,HEAT_INPUT,FLOW", 
        //        (String) data.get(0));
        //assertEquals("2161,**GT2,000113,19,0,-9,-9,-9,-9,-9,-9,-9", (String)data.get(21));
        //exportfile.delete(); */
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
    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
    
    protected TableReader tableReader(Datasource datasource) {
        return dbSetup.tableReader(datasource);
    }

}
