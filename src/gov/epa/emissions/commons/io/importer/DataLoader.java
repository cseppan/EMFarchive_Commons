package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataLoader {

    private Datasource datasource;

    private TableColumnsMetadata colsMetadata;

    public DataLoader(Datasource datasource, TableColumnsMetadata cols) {
        this.datasource = datasource;
        this.colsMetadata = cols;
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
    
    public void insertRow(Record record, Dataset dataset, String table) throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        if(!record.isEnd()) {
            modifier.insertRow(table, data(dataset, record), colsMetadata.cols());
        }
    }


    private void dropData(String table, Dataset dataset) throws ImporterException {
        try {
            DataModifier modifier = datasource.getDataModifier();
            String key = colsMetadata.key();
            long value = dataset.getDatasetid();
            modifier.dropData(table, key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = datasource.getDataModifier();
        while (!record.isEnd()) {
            modifier.insertRow(table, data(dataset, record), colsMetadata.cols());
            record = reader.read();
        }
    }
    
    private String[] data(Dataset dataset, Record record) {
        List data = new ArrayList();
        data.add("" + dataset.getDatasetid());
        for (int i = 0; i < record.size(); i++)
            data.add(record.token(i));

        return (String[]) data.toArray(new String[0]);
    }

}
