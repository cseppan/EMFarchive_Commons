package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataType;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.PacketReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TemporalProfileImporter {

    private SqlDataType sqlType;

    private Datasource datasource;

    public TemporalProfileImporter(Datasource datasource, SqlDataType sqlType) {
        this.datasource = datasource;
        this.sqlType = sqlType;
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(file));

            while (!isEndOfFile(fileReader)) {
                String header = readHeader(fileReader);
                ColumnsMetadata cols = colsMetadata(header);
                PacketReader reader = new PacketReader(fileReader, header, cols);
                DataLoader loader = new DataLoader(datasource, new TableColumnsMetadata(cols, sqlType));

                // Note: header is the same as table name
                loader.load(dataset, table(header), reader);
            }
        } catch (Exception e) {
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
        // FIXME: turn into a factory
        if (header.equals("MONTHLY"))
            return new MonthlyColumnsMetadata(sqlType);
        if (header.equals("WEEKLY"))
            return new WeeklyColumnsMetadata(sqlType);
        if (header.startsWith("DIURNAL"))
            return new DiurnalColumnsMetadata(sqlType);

        throw new ImporterException("invalid header - " + header);
    }

    private String readHeader(BufferedReader fileReader) throws IOException {
        String line = fileReader.readLine();
        return line.trim().replaceAll("/", "");
    }

}
