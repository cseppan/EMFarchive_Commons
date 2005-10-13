package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OptionalColumnsDataLoader {

    private Datasource datasource;

    private OptionalColumnsTableMetadata cols;

    public OptionalColumnsDataLoader(Datasource datasource, OptionalColumnsTableMetadata cols) {
        this.datasource = datasource;
        this.cols = cols;
    }

    // TODO: review if any of these params should go into the constructor
    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        try {
            insertRecords(dataset, table, reader);
        } catch (Exception e) {
            dropData(table, dataset);
            throw new ImporterException("could not load dataset - '" + dataset.getName() + "' into table - " + table, e);
        }
    }

    private void dropData(String table, Dataset dataset) throws ImporterException {
        try {
            DataModifier modifier = datasource.getDataModifier();
            String key = cols.key();
            long value = dataset.getDatasetid();
            modifier.dropData(table, key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        for (Record record = reader.read(); !record.isEnd(); record = reader.read()) {
            String[] types = cols.colTypes();
            String[] data = data(dataset, record, cols);
            modifier.insertRow(table, data, types);
        }
    }

    private String[] data(Dataset dataset, Record record, OptionalColumnsTableMetadata cols) {
        List data = new ArrayList();
        data.add("" + dataset.getDatasetid());
        data.addAll(record.tokens());

        if (data.size() < cols.colTypes().length)
            cols.addDefaultValuesForOptionals(data);

        return (String[]) data.toArray(new String[0]);
    }

}
