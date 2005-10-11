package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataLoader {

    private Datasource datasource;

    private ColumnsMetadata cols;

    public DataLoader(Datasource datasource, ColumnsMetadata cols) {
        this.datasource = datasource;
        this.cols = cols;
    }

    // TODO: review if any of these params should go into the constructor
    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        try {
            insertRecords(dataset, table, reader);
        } catch (Exception e) {
            dropData(table);
            throw new ImporterException("could not load dataset - '" + dataset.getName() + "' into table - " + table, e);
        }
    }

    private void dropData(String table) throws ImporterException {
        try {
            DataModifier modifier = datasource.getDataModifier();
            String qualifiedTable = qualifiedTableName(table);
            modifier.dropData(qualifiedTable);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private String qualifiedTableName(String table) {
        return datasource.getName() + "." + table;
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = datasource.getDataModifier();
        String qualifiedTable = qualifiedTableName(table);
        while (!record.isEnd()) {
            modifier.insertRow(qualifiedTable, data(dataset, record), cols.colTypes());
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
