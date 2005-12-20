package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DefaultVersionedRecordsWriter implements VersionedRecordsWriter {

    private PreparedStatement updateStatement;

    private String table;

    private VersionedDataFormatter dataFormatter;

    private Datasource datasource;

    public DefaultVersionedRecordsWriter(Datasource datasource, String table) throws SQLException {
        this.datasource = datasource;
        this.table = table;

        dataFormatter = dataFormatter(datasource, table);
        updateStatement = updateStatement(datasource, table);
    }

    private VersionedDataFormatter dataFormatter(Datasource datasource, String table) throws SQLException {
        DataModifier dataModifier = datasource.dataModifier();
        Column[] cols = dataModifier.getColumns(table);
        return new VersionedDataFormatter(cols);
    }

    private PreparedStatement updateStatement(Datasource datasource, String table) throws SQLException {
        String dataUpdate = "UPDATE " + datasource.getName() + "." + table + " SET delete_versions=? WHERE record_id=?";
        Connection connection = datasource.getConnection();
        return connection.prepareStatement(dataUpdate);
    }

    /**
     * ChangeSet contains adds, deletes, and updates. An update is treated as a combination of 'delete' and 'add'. In
     * effect, the ChangeSet is written as a list of 'delete' and 'add' operations.
     */
    public void update(ChangeSet changeset) throws Exception {
        convertUpdatedRecords(changeset);
        writeData(changeset);
    }

    public void close() throws SQLException {
        updateStatement.close();
    }

    private void convertUpdatedRecords(ChangeSet changeset) {
        VersionedRecord[] updatedRecords = changeset.getUpdatedRecords();

        for (int i = 0; i < updatedRecords.length; i++) {
            VersionedRecord deleteRec = updatedRecords[i];
            deleteRec.setDeleteVersions(deleteRec.getDeleteVersions() + "," + changeset.getVersion().getVersion());
            changeset.addDeleted(deleteRec);

            VersionedRecord insertRec = updatedRecords[i];
            changeset.addNew(insertRec);
        }
    }

    private void writeData(ChangeSet changeset) throws Exception {
        insertData(changeset.getNewRecords(), changeset.getVersion());
        deleteData(changeset.getDeletedRecords(), changeset.getVersion());
    }

    private void insertData(VersionedRecord[] records, Version version) throws Exception {
        DataModifier modifier = datasource.dataModifier();
        for (int i = 0; i < records.length; i++) {
            List data = dataFormatter.format(records[i], version.getVersion());
            String[] toArray = (String[]) data.toArray(new String[0]);
            modifier.insertRow(table, toArray);
        }
    }

    private void deleteData(VersionedRecord[] records, Version version) throws SQLException {
        for (int i = 0; i < records.length; i++) {
            updateStatement.setString(1, version.getVersion() + "");
            updateStatement.setInt(2, records[i].getRecordId());
            updateStatement.execute();
        }
    }

}
