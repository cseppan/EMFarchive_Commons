package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OptionalColumnsDataLoader implements DataLoader {

    private Datasource datasource;

    private TableFormatWithOptionalCols tableFormat;

    public OptionalColumnsDataLoader(Datasource datasource, TableFormatWithOptionalCols tableFormat) {
        this.datasource = datasource;
        this.tableFormat = tableFormat;
    }

    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        try {
            insertRecords(dataset, table, reader);
        } catch (Exception e) {
            dropData(table, dataset);
            throw new ImporterException(e.getMessage() + " could not load dataset - '" + dataset.getName() + "' into table - " + table);
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
            if (record.size() < tableFormat.minCols().length)
                throw new ImporterException("Dataset - " + dataset.getName() + " has a record " + record
                        + " that has less than number of minimum columns");

            String[] data = data(dataset, record, tableFormat);
            modifier.insertRow(table, data, tableFormat.cols());
        }
    }

    private String[] data(Dataset dataset, Record record, TableFormatWithOptionalCols colsMetadata) {
        List data = new ArrayList();
        data.add("" + dataset.getDatasetid());
        data.addAll(record.tokens());

        if (data.size() < colsMetadata.cols().length)
            colsMetadata.addDefaultValuesForOptionals(data);
        massageNullMarkers(data);

        return (String[]) data.toArray(new String[0]);
    }

    // FIXME: should this be applied to ALL data loaders ?
    private void massageNullMarkers(List data) {
        for (int i = 0; i < data.size(); i++) {
            String element = (String) data.get(i);
            if (element.equals("-9"))// NULL marker
                data.set(i, "");
        }
    }

}
