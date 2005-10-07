package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlTypeMapper;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MonthlyPacketLoader {

    private Datasource datasource;

    private SqlTypeMapper typeMapper;

    public MonthlyPacketLoader(Datasource datasource, SqlTypeMapper typeMapper) {
        this.datasource = datasource;
        this.typeMapper = typeMapper;
    }

    public void load(Dataset dataset, PacketReader reader) throws ImporterException {
        String table = reader.identify();
        try {
            createTable(table);
            insertRecords(table, dataset, reader);
        } catch (Exception e) {
            throw new ImporterException("could not load dataset - '" + dataset.getName() + "' into table - " + table, e);
        }
    }

    private void insertRecords(String table, Dataset dataset, PacketReader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = datasource.getDataModifier();
        String qualifiedTable = datasource.getName() + "." + table;
        while (!record.isEnd()) {
            modifier.insertRow(qualifiedTable, data(dataset, record), colTypes());
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

    private String[] colTypes() {
        String intType = typeMapper.getInt();
        String longType = typeMapper.getLong();

        String[] colTypes = { longType, intType, intType, intType, intType, intType, intType, intType, intType,
                intType, intType, intType, intType, intType, intType };

        return colTypes;
    }

    private void createTable(String table) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        String[] cols = { "Dataset_Id", "Code", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
                "Nov", "Dec", "Total_Weights" };

        tableDefinition.createTable(datasource.getName(), table, cols, colTypes());
    }
}
