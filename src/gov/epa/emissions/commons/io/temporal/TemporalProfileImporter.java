package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TemporalProfileImporter {

    private SqlDataTypes sqlType;

    private Datasource datasource;

    private TemporalFileFormatFactory metadataFactory;

    public TemporalProfileImporter(Datasource datasource, SqlDataTypes sqlType) {
        this.datasource = datasource;
        this.sqlType = sqlType;

        metadataFactory = new TemporalFileFormatFactory(sqlType);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(fileReader);

                FileFormat fileFormat = fileFormat(header);
                FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlType);
                DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);

                doImport(fileReader, dataset, unit, header);
            }
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void doImport(BufferedReader fileReader, Dataset dataset, DatasetTypeUnit unit, String header)
            throws ImporterException {
        Reader reader = new FixedWidthPacketReader(fileReader, header, unit.fileFormat());
        DataLoader loader = new FixedColumnsDataLoader(datasource, unit.tableFormat());
        // Note: header is the same as table name
        loader.load(reader, dataset, table(header));
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

}
