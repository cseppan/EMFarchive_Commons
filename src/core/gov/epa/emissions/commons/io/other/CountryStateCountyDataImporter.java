package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CountryStateCountyDataImporter implements Importer {

    private Dataset dataset;

    private SqlDataTypes sqlType;

    private Datasource datasource;

    private File file;
    
    private DataFormatFactory dataFormatFactory;

    private CountryStateCountyFileFormatFactory metadataFactory;

    public CountryStateCountyDataImporter(File folder, String[] filenames, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        this(folder, filenames, dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }
    
    public CountryStateCountyDataImporter(File folder, String[] filenames, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filenames);
        this.file = new File(folder, filenames[0]);
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.dataFormatFactory = factory;
        this.sqlType = sqlDataTypes;
        metadataFactory = new CountryStateCountyFileFormatFactory(sqlType);
    }

    public void run() throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(dataset, fileReader);
                FileFormat fileFormat = fileFormat(header);
                TableFormat tableFormat = dataFormatFactory.tableFormat(fileFormat, sqlType);
                DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);

                DataTable dataTable = new DataTable(dataset, datasource);
                String table = table(header);
                if(!dataTable.exists(table))
                    dataTable.create(table, unit.tableFormat());
                
                doImport(fileReader, dataset, unit, header);
            }
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName()+"\n"+e.getMessage());
        }
    }

    private void doImport(BufferedReader fileReader, Dataset dataset, DatasetTypeUnit unit, String header)
            throws ImporterException {
        Reader reader = new CountryStateCountyFileReader(fileReader, header,
                new InventoryTableParser(unit.fileFormat()));
        DataLoader loader = new FixedColumnsDataLoader(datasource, unit.tableFormat());
        // Note: header is the same as table name
        loader.load(reader, dataset, table(header));
        loadDataset(file, table(header), unit.tableFormat(), dataset);
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
        if (datasetdesc != null)
            descrptn += datasetdesc;

        // In case first line is not a beginning of a packet, esp. when called
        // the
        // first time
        while (!line.trim().startsWith("/")) {
            descrptn += line;
            line = fileReader.readLine();
        }

        if (!descrptn.equalsIgnoreCase(""))
            dataset.setDescription(descrptn);

        return line.trim().replaceAll("/", "");
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
    }

}
