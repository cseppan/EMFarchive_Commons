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
            throw new ImporterException("Line number " + reader.lineNumber() + ": " + e.getMessage() + "\nLine: "
                    + reader.line() + "\nCould not load dataset - '" + dataset.getName() + "' into table - " + table);
        }
    }

    private void dropData(String table, Dataset dataset) throws ImporterException {
        try {
            DataModifier modifier = datasource.dataModifier();
            String key = tableFormat.key();
            long value = dataset.getDatasetid();
            modifier.dropData(table, key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table + "\n" + e.getMessage(), e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        DataModifier modifier = datasource.dataModifier();
        for (Record record = reader.read(); !record.isEnd(); record = reader.read()) {
            int minColsSize = tableFormat.minCols().length;
            if (record.size() < minColsSize)
                throw new ImporterException("The number of tokens in the line are " + record.size()
                        + ", It's less than minimum number of columns expected(" + minColsSize + ")");

            String[] data = data(dataset, record, tableFormat);
            try {
                modifier.insertRow(table, data, tableFormat.cols());
            } catch (SQLException e) {
                throw new ImporterException("Error in inserting query\n" + e.getMessage());
            }
        }
    }

    private String[] data(Dataset dataset, Record record, TableFormatWithOptionalCols tableFormat) {
        List data = new ArrayList();
        data.addAll(record.tokens());

        tableFormat.fillDefaults(data, dataset.getDatasetid());
        
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
