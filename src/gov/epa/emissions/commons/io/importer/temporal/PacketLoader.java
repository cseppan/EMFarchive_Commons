package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.util.ArrayList;
import java.util.List;

public class PacketLoader {

    private Datasource datasource;

    private ColumnsMetadata cols;

    public PacketLoader(Datasource datasource, ColumnsMetadata cols) {
        this.datasource = datasource;
        this.cols = cols;
    }

    public void load(Dataset dataset, PacketReader reader) throws ImporterException {
        String table = toTableName(reader);
        try {
            insertRecords(table, dataset, reader);
        } catch (Exception e) {
            throw new ImporterException("could not load dataset - '" + dataset.getName() + "' into table - " + table, e);
        }
    }

    private String toTableName(PacketReader reader) {
        String identifier = reader.identify();
        return identifier.replaceAll(" ", "_");
    }

    private void insertRecords(String table, Dataset dataset, PacketReader reader) throws Exception {
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
