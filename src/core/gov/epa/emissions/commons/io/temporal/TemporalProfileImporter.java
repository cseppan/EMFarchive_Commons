package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

public class TemporalProfileImporter implements Importer {

    private SqlDataTypes sqlType;

    private Datasource datasource;

    private TemporalFileFormatFactory metadataFactory;

    private Dataset dataset;

    private File file;

    public TemporalProfileImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlType) {
        this.datasource = datasource;
        this.sqlType = sqlType;
        this.dataset = dataset;
        this.file = file;

        metadataFactory = new TemporalFileFormatFactory(sqlType);
    }

    public void run() throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(fileReader);

                FileFormat fileFormat = fileFormat(header);
                FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlType);
                DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);

                doImport(fileReader, unit, header);
            }
            addVersionZeroEntryToVersionsTable(datasource, dataset);
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void doImport(BufferedReader fileReader, DatasetTypeUnit unit, String header) throws Exception {
        DataTable dataTable = new DataTable(dataset);
        try {
            dataTable.create(table(header), datasource, unit.tableFormat());
            doImport(fileReader, dataset, unit, header);
        } catch (Exception e) {
            dataTable.drop(table(header), datasource);
            throw e;
        }
    }

    private void doImport(BufferedReader fileReader, Dataset dataset, DatasetTypeUnit unit, String header)
            throws Exception {
        Reader reader = new FixedWidthPacketReader(fileReader, header, unit.fileFormat());
        DataLoader loader = new FixedColumnsDataLoader(datasource, unit.tableFormat());
        // Note: header is the same as table name
        loader.load(reader, dataset, table(header));
        loadDataset(file, table(header), unit.tableFormat(), dataset);
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
        String[] data = { dataset.getDatasetid() + "", "0", "Initial Version", "", "true", null };
        modifier.insertRow("versions", data);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
    }

}
