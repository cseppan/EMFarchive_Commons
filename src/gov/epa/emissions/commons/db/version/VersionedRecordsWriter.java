package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class VersionedRecordsWriter {

    private Datasource datasource;

    private PreparedStatement dataInsertStatement;

    private PreparedStatement versionsInsertStatement;

    public VersionedRecordsWriter(Datasource datasource) throws SQLException {
        this.datasource = datasource;
        createPreparedStatments(datasource);
    }

    private void createPreparedStatments(Datasource datasource) throws SQLException {
        Connection connection = datasource.getConnection();

        String dataInsert = "INSERT INTO emissions.data (record_id,dataset_id,version,delete_version) VALUES (default,?,?,?)";
        dataInsertStatement = connection.prepareStatement(dataInsert);

        String versionsInsert = "INSERT INTO emissions.versions (dataset_id,version,parent_versions) VALUES (?,?,?)";
        versionsInsertStatement = connection.prepareStatement(versionsInsert);
    }

    public Version write(ChangeSet changeset) {
        Version version = insertNewVersion(changeset.getParentVersion());

        insertNewData(changeset.getRecords());

        return version;
    }

    private void insertNewData(VersionedRecord[] records) {
        // TODO Auto-generated method stub
    }

    private Version insertNewVersion(Version parentVersion) {
        return null;
    }

    public void close() throws SQLException {
        dataInsertStatement.close();
        versionsInsertStatement.close();
    }

}
