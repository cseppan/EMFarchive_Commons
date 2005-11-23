package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class VersionedRecordsWriter {

    private Datasource datasource;

    private PreparedStatement dataInsertStatement;

    private PreparedStatement versionsInsertStatement;

    private PreparedStatement versionNumberStatement;

    public VersionedRecordsWriter(Datasource datasource) throws SQLException {
        this.datasource = datasource;
        createPreparedStatments(datasource);
    }

    private void createPreparedStatments(Datasource datasource) throws SQLException {
        Connection connection = datasource.getConnection();

        String dataInsert = "INSERT INTO emissions.data (record_id,dataset_id,version) VALUES (default,?,?)";
        dataInsertStatement = connection.prepareStatement(dataInsert);

        String versionsInsert = "INSERT INTO emissions.versions (dataset_id,version,parent_versions) VALUES (?,?,?)";
        versionsInsertStatement = connection.prepareStatement(versionsInsert);

        String selectVersionNumber = "select version from emissions.versions where dataset_id=? order by version";
        versionNumberStatement = connection.prepareStatement(selectVersionNumber, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

    }

    public Version write(ChangeSet changeset) throws Exception {
        Version version = insertNewVersion(changeset.getBaseVersion());
        insertNewData(changeset.getRecords(), version);

        return version;
    }

    private void insertNewData(VersionedRecord[] records, Version version) throws Exception {
        for (int i = 0; i < records.length; i++) {
            dataInsertStatement.setInt(1, records[i].getDatasetId());
            dataInsertStatement.setInt(2, version.getVersion());
            dataInsertStatement.execute();
        }
    }

    private Version insertNewVersion(Version baseVersion) throws Exception {
        Version version = new Version();

        // get the last version number for this dataset
        int newVersionNum = getNextVersionNumber(baseVersion.getDatasetId());
        if (baseVersion.getParentVersions().length() == 0)
            version.setParentVersions(baseVersion.getVersion() + "");
        else
            version.setParentVersions(baseVersion.getParentVersions() + "," + baseVersion.getVersion());

        version.setDatasetId(baseVersion.getDatasetId());
        version.setVersion(newVersionNum);

        versionsInsertStatement.setInt(1, baseVersion.getDatasetId());
        versionsInsertStatement.setInt(2, newVersionNum);
        versionsInsertStatement.setString(3, version.getParentVersions());
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
    }

}
