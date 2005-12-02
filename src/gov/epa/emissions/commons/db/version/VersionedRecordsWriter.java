package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class VersionedRecordsWriter {

    private PreparedStatement dataInsertStatement;

    private PreparedStatement dataDeleteStatement;

    private Versions versions;

    public VersionedRecordsWriter(Datasource datasource) throws SQLException {
        Connection connection = datasource.getConnection();

        String dataInsert = "INSERT INTO " + datasource.getName()
                + ".data (record_id,dataset_id,version) VALUES (default,?,?)";
        dataInsertStatement = connection.prepareStatement(dataInsert);

        String dataDelete = "UPDATE " + datasource.getName() + ".data SET delete_versions=? WHERE record_id=?";
        dataDeleteStatement = connection.prepareStatement(dataDelete);

        versions = new Versions(datasource);
    }

    /**
     * ChangeSet contains adds, deletes, and updates. An update is treated as a
     * combination of 'delete' and 'add'. In effect, the ChangeSet is written as
     * a list of 'delete' and 'add' operations.
     */
    public void update(ChangeSet changeset) throws Exception {
        convertUpdatedRecords(changeset);
        writeData(changeset, changeset.getVersion());
    }

    /**
     * Inserts the ChangeSet and marks the 'version' as Final
     */
    public Version writeFinal(ChangeSet changeset) throws Exception {
        update(changeset);
        return versions.insertFinalVersion(changeset.getVersion());
    }

    public void close() throws SQLException {
        dataInsertStatement.close();
        dataDeleteStatement.close();
        versions.close();
    }

    private void convertUpdatedRecords(ChangeSet changeset) {
        VersionedRecord[] updatedRecords = changeset.getUpdated();

        for (int i = 0; i < updatedRecords.length; i++) {
            VersionedRecord deleteRec = updatedRecords[i];
            deleteRec.setDeleteVersions(deleteRec.getDeleteVersions() + "," + changeset.getVersion().getVersion());
            changeset.addDeleted(deleteRec);

            VersionedRecord insertRec = updatedRecords[i];
            changeset.addNew(insertRec);
        }
    }

    private void writeData(ChangeSet changeset, Version version) throws Exception {
        insertData(changeset.getNew(), version);
        deleteData(changeset.getDeleted(), version);
    }

    private void insertData(VersionedRecord[] records, Version version) throws Exception {
        for (int i = 0; i < records.length; i++) {
            dataInsertStatement.setInt(1, records[i].getDatasetId());
            dataInsertStatement.setInt(2, version.getVersion());
            dataInsertStatement.execute();
        }
    }

    private void deleteData(VersionedRecord[] records, Version version) throws SQLException {
        for (int i = 0; i < records.length; i++) {
            dataDeleteStatement.setString(1, version.getVersion() + "");
            dataDeleteStatement.setInt(2, records[i].getRecordId());
            dataDeleteStatement.execute();
        }
    }

}
