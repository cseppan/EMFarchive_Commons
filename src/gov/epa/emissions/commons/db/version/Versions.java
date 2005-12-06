package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;

public class Versions {

    private Datasource datasource;

    private PreparedStatement insertStatement;

    private PreparedStatement nextVersionStatement;

    private PreparedStatement updateStatement;

    private PreparedStatement versionsStatement;

    public Versions(Datasource datasource) throws SQLException {
        this.datasource = datasource;

        Connection connection = datasource.getConnection();

        String insert = "INSERT INTO " + datasource.getName()
                + ".versions (dataset_id,version,name, path) VALUES (?,?,?,?)";
        insertStatement = connection.prepareStatement(insert);

        String update = "UPDATE " + datasource.getName() + ".versions set final_version=true WHERE dataset_id=?";
        updateStatement = connection.prepareStatement(update);

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

    public Version[] get(long datasetId) throws SQLException {
        // FIXME: convert to long
        versionsStatement.setInt(1, (int) datasetId);
        ResultSet rs = versionsStatement.executeQuery();

        List versions = new ArrayList();
        while (rs.next()) {
            Version version = new Version();
            version.setDatasetId(rs.getLong("Dataset_Id"));
            version.setVersion(rs.getInt("Version"));
            version.setPath(rs.getString("Path"));
            if (rs.getBoolean("final_version"))
                version.markFinal();

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

        insertStatement.setLong(1, version.getDatasetId());
        insertStatement.setInt(2, version.getVersion());
        insertStatement.setString(3, version.getName());
        insertStatement.setString(4, version.getPath());
        insertStatement.executeUpdate();

        return version;
    }

    public Version markFinal(Version derived) throws SQLException {
        derived.markFinal();
        updateStatement.setLong(1, derived.getDatasetId());
        updateStatement.executeUpdate();

        return derived;
    }

    private String path(Version base) {
        String path = base.getPath().length() == 0 ? (base.getVersion() + "") : (base.getPath() + "," + base
                .getVersion());
        return path;
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
        updateStatement.close();
        versionsStatement.close();
    }

}
