package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CountryStateCountyDataImporter implements Importer {
    private SqlDataTypes sqlType;

    private Datasource datasource;
    
    private HelpImporter delegate;
    
    private File file;

    private CountryStateCountyFileFormatFactory metadataFactory;

    public CountryStateCountyDataImporter(Datasource datasource, SqlDataTypes sqlType) {
        this.datasource = datasource;
        this.sqlType = sqlType;

        metadataFactory = new CountryStateCountyFileFormatFactory(sqlType);
        this.delegate = new HelpImporter();
    }
    
    public void preCondition(File folder, String filePattern) {
        File file = new File(folder, filePattern);
        try{   
            delegate.validateFile(file);
        }catch (ImporterException e){
            e.printStackTrace();
        }
        this.file = file;
    }

    public void run(Dataset dataset) throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(dataset, fileReader);
                FileFormat fileFormat = fileFormat(header);
                FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlType);
                DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);

                delegate.createTable(table(header), datasource, unit.tableFormat(), header);
                doImport(fileReader, dataset, unit, header);
            }
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void doImport(BufferedReader fileReader, Dataset dataset, DatasetTypeUnit unit, String header)
            throws ImporterException {
        Reader reader = new CountryStateCountyFileReader(fileReader, header, new InventoryTableParser(unit.fileFormat()));
        DataLoader loader = new FixedColumnsDataLoader(datasource, unit.tableFormat());
        // Note: header is the same as table name
        loader.load(reader, dataset, table(header));
        loadDataset(file, table(header), unit.fileFormat(), dataset);
    }

    // TODO: revisit ?
    private String table(String header) {
        return header.replace(' ', '_');
    }

    private boolean isEndOfFile(BufferedReader fileReader) throws IOException {
        return !fileReader.ready();
    }

    private FileFormat fileFormat(String header) throws ImporterException {
        FileFormat meta = metadataFactory.get(header);
        if (meta == null)
            throw new ImporterException("invalid header - " + header);
        
        return meta;
    }

    private String readHeader(Dataset dataset, BufferedReader fileReader) throws IOException {
        String line = fileReader.readLine();
        String descrptn = "";
        String datasetdesc = dataset.getDescription();
        if(datasetdesc != null)
            descrptn += datasetdesc;

        //In case first line is not a beginning of a packet, esp. when called the
        //first time
        while (!line.trim().startsWith("/")){
            descrptn += line;
            line = fileReader.readLine();
        }
        
        if(!descrptn.equalsIgnoreCase(""))
            dataset.setDescription(descrptn);
        
        return line.trim().replaceAll("/", "");
    }
    
    private void loadDataset(File file, String table, FileFormat fileFormat, Dataset dataset) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
    }

}