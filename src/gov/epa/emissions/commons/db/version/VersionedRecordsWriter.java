package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class VersionedRecordsWriter {

    private PreparedStatement dataInsertStatement;

    private PreparedStatement versionsInsertStatement;

    private PreparedStatement versionNumberStatement;

    private PreparedStatement dataDeleteStatement;

    public VersionedRecordsWriter(Datasource datasource) throws SQLException {
        Connection connection = datasource.getConnection();

        String dataInsert = "INSERT INTO emissions.data (record_id,dataset_id,version) VALUES (default,?,?)";
        dataInsertStatement = connection.prepareStatement(dataInsert);

        String dataDelete = "UPDATE emissions.data SET delete_versions=? WHERE record_id=?";
        dataDeleteStatement = connection.prepareStatement(dataDelete);

        String versionsInsert = "INSERT INTO emissions.versions (dataset_id,version,path) VALUES (?,?,?)";
        versionsInsertStatement = connection.prepareStatement(versionsInsert);

        String selectVersionNumber = "SELECT version FROM emissions.versions WHERE dataset_id=? ORDER BY version";
        versionNumberStatement = connection.prepareStatement(selectVersionNumber, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

    }

    /**
     * ChangeSet contains adds, deletes, and updates. An update is treated as a
     * combination of 'delete' and 'add'. In effect, the ChangeSet is written as
     * a list of 'delete' and 'add' operations.
     */
    public Version write(ChangeSet changeset) throws Exception {
        VersionedRecord[] updatedRecords = changeset.getUpdated();

        for (int i = 0; i < updatedRecords.length; i++) {
            VersionedRecord deleteRec = updatedRecords[i];
            deleteRec.setDeleteVersions(deleteRec.getDeleteVersions() + "," + changeset.getBaseVersion().getVersion());
            changeset.addDeleted(deleteRec);
            
            VersionedRecord insertRec = updatedRecords[i];
            changeset.addNew(insertRec);
        }

        return doWrite(changeset);
    }

    private Version doWrite(ChangeSet changeset) throws Exception {
        Version version = insertNewVersion(changeset.getBaseVersion());
        insertNewData(changeset.getNew(), version);
        deleteData(changeset.getDeleted(), version);

        return version;
    }

    private void insertNewData(VersionedRecord[] records, Version version) throws Exception {
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

    private Version insertNewVersion(Version baseVersion) throws Exception {
        Version version = new Version();

        // get the last version number for this dataset
        int newVersionNum = getNextVersionNumber(baseVersion.getDatasetId());
        if (baseVersion.getPath().length() == 0)
            version.setPath(baseVersion.getVersion() + "");
        else
            version.setPath(baseVersion.getPath() + "," + baseVersion.getVersion());

        version.setDatasetId(baseVersion.getDatasetId());
        version.setVersion(newVersionNum);

        versionsInsertStatement.setInt(1, baseVersion.getDatasetId());
        versionsInsertStatement.setInt(2, newVersionNum);
        versionsInsertStatement.setString(3, version.getPath());
        versionsInsertStatement.executeUpdate();

        return version;
    }

    private int getNextVersionNumber(int datasetId) throws Exception {
        versionNumberStatement.setInt(1, datasetId);
        ResultSet rs = versionNumberStatement.executeQuery();
        rs.last();

        return rs.getInt("version") + 1;
    }

    public void close() throws SQLException {
        dataInsertStatement.close();
        versionsInsertStatement.close();
        versionNumberStatement.close();
        dataDeleteStatement.close();
    }

}
