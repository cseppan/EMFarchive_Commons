package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class VersionedDataLoader implements DataLoader {
    private Datasource datasource;

    private FileFormatWithOptionalCols fileFormat;

    private String key;

    public VersionedDataLoader(Datasource datasource, FileFormatWithOptionalCols format, String key) {
        this.datasource = datasource;
        this.fileFormat = format;
        this.key = key;
    }

    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        try {
            insertRecords(dataset, table, reader);
        } catch (Exception e) {
            dropData(table, dataset);
            throw new ImporterException("Line number " + reader.lineNumber() + ": " + e.getMessage()
                    + "\nCould not load dataset - '" + dataset.getName() + "' into table - " + table);
        }
    }

    private void dropData(String table, Dataset dataset) throws ImporterException {
        try {
            DataModifier modifier = datasource.dataModifier();
            long value = dataset.getDatasetid();
            modifier.dropData(table, key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = datasource.dataModifier();
        while (!record.isEnd()) {
            modifier.insertRow(table, data(dataset, record, fileFormat));
            record = reader.read();
        }
    }

    private String[] data(Dataset dataset, Record record, FileFormatWithOptionalCols fileFormat) {
        List data = new ArrayList();
        data.addAll(record.tokens());
        fileFormat.fillDefaults(data, dataset.getDatasetid());
        massageNullMarkers(data);

        return (String[]) data.toArray(new String[0]);
    }

    // FIXME: should this be applied to ALL data loaders ?
    private void massageNullMarkers(List data) {
        for (int i = 0; i < data.size(); i++) {
            String element = (String) data.get(i);
            if (element.equals("-9"))// NULL marker
                data.set(i, null);
        }
    }

}
