package gov.epa.emissions.commons.io.importer.generic;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.Record;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LineLoader implements DataLoader{
    private Datasource datasource;
    private TableFormat tableFormat;

    public LineLoader(Datasource datasource, TableFormat tableFormat) {
        this.datasource = datasource;
        this.tableFormat = tableFormat;
    }

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
            String key = tableFormat.key();
            long value = dataset.getDatasetid();
            modifier.dropData(table, key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        for (Record record = reader.read(); !record.isEnd(); record = reader.read()) {
            String[] data = data(dataset, record);
            modifier.insertRow(table, data, tableFormat.cols());
        }
    }

    private String[] data(Dataset dataset, Record record) {
        List data = new ArrayList();
        data.add("" + dataset.getDatasetid());
        data.addAll(record.tokens());

        return (String[]) data.toArray(new String[0]);
    }

}
