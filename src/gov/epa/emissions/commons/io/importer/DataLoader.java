package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;

import java.util.ArrayList;
import java.util.List;

public class DataLoader {

    private Datasource datasource;

    private ColumnsMetadata cols;

    public DataLoader(Datasource datasource, ColumnsMetadata cols) {
        this.datasource = datasource;
        this.cols = cols;
    }

    public void load(Dataset dataset, String table, Reader reader) throws ImporterException {
        try {
            insertRecords(dataset, table, reader);
        } catch (Exception e) {
            throw new ImporterException("could not load dataset - '" + dataset.getName() + "' into table - " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = datasource.getDataModifier();
        String qualifiedTable = datasource.getName() + "." + table;
        while (!record.isEnd()) {
            modifier.insertRow(qualifiedTable, data(dataset, record), cols.colTypes());
            record = reader.read();
        }
    }

    private String[] data(Dataset dataset, Record record) {
        List data = new ArrayList();
        data.add("" + dataset.getDatasetid());
        for (int i = 0; i < record.size(); i++) {
            data.add(record.token(i));
        }

        return (String[]) data.toArray(new String[0]);
    }

}
