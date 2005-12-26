package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.TableFormat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FixedColumnsDataLoader implements DataLoader {

    private Datasource datasource;

    private TableFormat tableFormat;

    public FixedColumnsDataLoader(Datasource datasource, TableFormat tableFormat) {
        this.datasource = datasource;
        this.tableFormat = tableFormat;
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
            String key = tableFormat.key();
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
            modifier.insertRow(table, data(dataset, record), tableFormat.cols());
            record = reader.read();
        }
    }

    private String[] data(Dataset dataset, Record record) {
        List data = new ArrayList();
        data.add("" + dataset.getDatasetid());
        for (int i = 0; i < record.size(); i++) {
            data.add(record.token(i));
        }
        addToEnd(data);
        return (String[]) data.toArray(new String[0]);
    }

    private void addToEnd(List data) {
        int remain = tableFormat.cols().length - data.size();
        for (int i = 0; i < remain; i++) {
            data.add("");
        }
    }

}