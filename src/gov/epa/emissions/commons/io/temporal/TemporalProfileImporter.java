package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TemporalProfileImporter implements Importer {

    private static Log log = LogFactory.getLog(PointTemporalReferenceImporter.class);
    
    private SqlDataTypes sqlType;

    private Datasource datasource;
    
    private HelpImporter delegate;

    private TemporalFileFormatFactory metadataFactory;
    
    private Dataset dataset;
    
    private File file;

    public TemporalProfileImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlType) 
            throws ImporterException {
        this.datasource = datasource;
        this.sqlType = sqlType;
        this.dataset = dataset;
        setup(file);

        metadataFactory = new TemporalFileFormatFactory(sqlType);
        delegate = new HelpImporter();
    }
    
    private void setup(File file) throws ImporterException {
        this.file = validateFile(file);
    }

    private File validateFile(File file) throws ImporterException {
        log.debug("check if file exists " + file.getAbsolutePath());
        if (!file.exists() || !file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new ImporterException("File not found");
        }
        return file;
    }

    public void run() throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(fileReader);

                FileFormat fileFormat = fileFormat(header);
                VersionedTableFormat tableFormat = new VersionedTableFormat(fileFormat, sqlType);
                DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);

                delegate.createTable(table(header), datasource, unit.tableFormat(), dataset.getName());
                doImport(fileReader, dataset, unit, header);
            }
            addVersionZeroEntryToVersionsTable(datasource, dataset);
        } catch (Exception e) {
            //delegate.dropTable(table(header), datasource);
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void doImport(BufferedReader fileReader, Dataset dataset, DatasetTypeUnit unit, String header)
            throws Exception {
        Reader reader = new FixedWidthPacketReader(fileReader, header, unit.fileFormat());
        DataLoader loader = new VersionedDataLoader(datasource, (VersionedTableFormat) unit.tableFormat());
        // Note: header is the same as table name
        loader.load(reader, dataset, table(header));
        loadDataset(file, table(header), unit.fileFormat(), dataset);
    }

    // TODO: revisit ?
    private String table(String header) {
        return header.replaceAll(" ", "_");
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

    private String readHeader(BufferedReader fileReader) throws IOException {
        String line = fileReader.readLine();
        return line.trim().replaceAll("/", "");
    }
    
    private void addVersionZeroEntryToVersionsTable(Datasource datasource, Dataset dataset) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        String[] data = { dataset.getDatasetid() + "", "0", "Initial Version", "", "true" };
        modifier.insertRow("versions", data);
    }
    
    private void loadDataset(File file, String table, FileFormat fileFormat, Dataset dataset) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
    }

}
