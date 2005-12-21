package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;

public class Versions {

    private Datasource datasource;

    private PreparedStatement insertStatement;

    private PreparedStatement nextVersionStatement;

    private PreparedStatement markFinalStatement;

    private PreparedStatement versionsStatement;

    public Versions(Datasource datasource) throws SQLException {
        this.datasource = datasource;

        Connection connection = datasource.getConnection();
        connection.setAutoCommit(true);
        createPreparedStatements(datasource, connection);
    }

    private void createPreparedStatements(Datasource datasource, Connection connection) throws SQLException {
        String insert = "INSERT INTO " + datasource.getName()
                + ".versions (dataset_id,version,name, path,date) VALUES (?,?,?,?,?)";
        insertStatement = connection.prepareStatement(insert);

        String markFinal = "UPDATE " + datasource.getName()
                + ".versions SET final_version=true AND date=? WHERE dataset_id=? AND version=?";
        markFinalStatement = connection.prepareStatement(markFinal);

        String selectVersionNumber = "SELECT version FROM " + datasource.getName()
                + ".versions WHERE dataset_id=? ORDER BY version";
        nextVersionStatement = connection.prepareStatement(selectVersionNumber, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        String selectVersions = "SELECT * FROM " + datasource.getName()
                + ".versions WHERE dataset_id=? ORDER BY version";
        versionsStatement = connection.prepareStatement(selectVersions, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }

    public Version[] getPath(long datasetId, int finalVersion) throws SQLException {
        ResultSet rs = queryVersion(datasetId, finalVersion);
        if (!rs.next())
            return new Version[0];

        return doGetPath(datasetId, rs);
    }

    private Version[] doGetPath(long datasetId, ResultSet rs) throws SQLException {
        int[] parentVersions = parseParentVersions(rs.getString("path"));
        List versions = new ArrayList();
        for (int i = 0; i < parentVersions.length; i++) {
            Version parent = get(datasetId, parentVersions[i]);
            versions.add(parent);
        }

        versions.add(extractVersion(rs));

        return (Version[]) versions.toArray(new Version[0]);
    }

    public Version get(long datasetId, int version) throws SQLException {
        ResultSet rs = queryVersion(datasetId, version);
        if (!rs.next())
            return null;

        return extractVersion(rs);
    }

    private ResultSet queryVersion(long datasetId, int version) throws SQLException {
        DataQuery query = datasource.query();
        ResultSet rs = query.executeQuery("SELECT * FROM " + datasource.getName() + ".versions WHERE dataset_id = "
                + datasetId + " AND version = " + version);

        return rs;
    }

    private Version extractVersion(ResultSet rs) throws SQLException {
        Version version = new Version();
        version.setDatasetId(rs.getLong("dataset_id"));
        version.setVersion(rs.getInt("version"));
        version.setName(rs.getString("name"));
        version.setPath(rs.getString("path"));
        if (rs.getBoolean("final_version"))
            version.markFinal();
        version.setDate(rs.getTimestamp("date"));

        return version;
    }

    private int[] parseParentVersions(String versionsList) {
        IntList versions = new ArrayIntList();

        StringTokenizer tokenizer = new StringTokenizer(versionsList, ",");
        while (tokenizer.hasMoreTokens()) {
            int token = Integer.parseInt(tokenizer.nextToken());
            versions.add(token);
        }

        return versions.toArray();
    }

    public int getLastFinalVersion(long datasetId) throws SQLException {
        int versionNumber = 0;

        Version[] allVersionForDataset = get(datasetId);

        for (int i = 0; i < allVersionForDataset.length; i++) {
            int versNum = allVersionForDataset[i].getVersion();

            if (versNum > versionNumber) {
                versionNumber = allVersionForDataset[i].getVersion();
            }
        }

        return versionNumber;
    }

    public Version[] get(long datasetId) throws SQLException {
        // FIXME: convert to long
        versionsStatement.setInt(1, (int) datasetId);
        ResultSet rs = versionsStatement.executeQuery();

        List versions = new ArrayList();
        while (rs.next()) {
            Version version = extractVersion(rs);
            versions.add(version);
        }

        return (Version[]) versions.toArray(new Version[0]);
    }

    public Version derive(Version base, String name) throws SQLException {
        if (!base.isFinalVersion())
            throw new RuntimeException("cannot derive a new version from a non-final version");

        Version version = new Version();
        int newVersionNum = getNextVersionNumber(base.getDatasetId());

        version.setName(name);
        version.setVersion(newVersionNum);
        version.setPath(path(base));
        version.setDatasetId(base.getDatasetId());
        version.setDate(new Date());
        // version.setCreator(user); TODO

        insertStatement.setLong(1, version.getDatasetId());
        insertStatement.setInt(2, version.getVersion());
        insertStatement.setString(3, version.getName());
        insertStatement.setString(4, version.getPath());
        insertStatement.setTimestamp(5, new Timestamp(version.getDate().getTime()));

        insertStatement.executeUpdate();

        return get(version.getDatasetId(), version.getVersion());
    }

    public Version markFinal(Version derived) throws SQLException {
        derived.markFinal();
        derived.setDate(new Date());

        // FIXME: need to add 'date' to the update statement
        String update = "UPDATE " + datasource.getName() + ".versions SET final_version=true WHERE dataset_id="
                + derived.getDatasetId() + " AND version=" + derived.getVersion();

        Statement stmt = datasource.getConnection().createStatement();
        stmt.executeUpdate(update);

        return get(derived.getDatasetId(), derived.getVersion());
    }

    private String path(Version base) {
        return base.createPathForDerived();
    }

    private int getNextVersionNumber(long datasetId) throws SQLException {
        nextVersionStatement.setLong(1, datasetId);
        ResultSet rs = nextVersionStatement.executeQuery();
        rs.last();

        return rs.getInt("version") + 1;
    }

    public void close() throws SQLException {
        insertStatement.close();
        nextVersionStatement.close();
        markFinalStatement.close();
        versionsStatement.close();
    }

}
