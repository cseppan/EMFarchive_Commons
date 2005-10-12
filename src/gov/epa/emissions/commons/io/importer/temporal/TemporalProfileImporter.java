package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DataLoader;
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

    private TemporalColumnsMetadataFactory metadataFactory;

    public TemporalProfileImporter(Datasource datasource, SqlDataTypes sqlType) {
        this.datasource = datasource;
        this.sqlType = sqlType;

        metadataFactory = new TemporalColumnsMetadataFactory(sqlType);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(fileReader);
                ColumnsMetadata cols = colsMetadata(header);
                Reader reader = new FixedWidthPacketReader(fileReader, header, cols);
                DataLoader loader = new DataLoader(datasource, new TableColumnsMetadata(cols, sqlType));

                // Note: header is the same as table name
                loader.load(reader, dataset, table(header));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    // TODO: revisit ?
    private String table(String header) {
        return header.replaceAll(" ", "_");
    }

    private boolean isEndOfFile(BufferedReader fileReader) throws IOException {
        return !fileReader.ready();
    }

    private ColumnsMetadata colsMetadata(String header) throws ImporterException {
        ColumnsMetadata meta = metadataFactory.get(header);
        if (meta == null)
            throw new ImporterException("invalid header - " + header);

        return meta;
    }

    private String readHeader(BufferedReader fileReader) throws IOException {
        String line = fileReader.readLine();
        return line.trim().replaceAll("/", "");
    }

}
