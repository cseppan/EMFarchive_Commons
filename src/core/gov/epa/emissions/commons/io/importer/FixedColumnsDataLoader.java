package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

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
            long value = dataset.getId();
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

    private String[] data(Dataset dataset, Record record){
        List data = new ArrayList();
  
        if (tableFormat instanceof VersionedTableFormat)
            addVersionData(data, dataset.getId(), 0);
        else
            data.add("" + dataset.getId());
  
        data.addAll(record.tokens());

        massageNullMarkers(data);
  
        return (String[]) data.toArray(new String[0]);
    }

    private void addVersionData(List data, long datasetId, int version) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version + "");// version
        data.add(3, "");// delete versions
    }

    private void massageNullMarkers(List data) {
        for (int i = 0; i < data.size(); i++) {
            String element = (String) data.get(i);
            if (element.equals("-9"))// NULL marker
                data.set(i, "");
        }
    }

}
