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

    private PreparedStatement insertFinalStatement;

    public Versions(Datasource datasource) throws SQLException {
        this.datasource = datasource;

        Connection connection = datasource.getConnection();

        String versionsInsert = "INSERT INTO " + datasource.getName()
                + ".versions (dataset_id,version,path) VALUES (?,?,?)";
        insertStatement = connection.prepareStatement(versionsInsert);

        String insertFinal = "INSERT INTO " + datasource.getName()
                + ".versions (dataset_id,version,path,final_version) VALUES (?,?,?,true)";
        insertFinalStatement = connection.prepareStatement(insertFinal);

        String selectVersionNumber = "SELECT version FROM " + datasource.getName()
                + ".versions WHERE dataset_id=? ORDER BY version";
        nextVersionStatement = connection.prepareStatement(selectVersionNumber, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }

    public Version[] getPath(int datasetId, int finalVersion) throws SQLException {
        ResultSet rs = queryVersion(datasetId, finalVersion);
        if (!rs.next())
            return new Version[0];

        return doGetPath(datasetId, rs);
    }

    private Version[] doGetPath(int datasetId, ResultSet rs) throws SQLException {
        int[] parentVersions = parseParentVersions(rs.getString(3));
        List versions = new ArrayList();
        for (int i = 0; i < parentVersions.length; i++) {
            Version parent = fetchVersion(datasetId, parentVersions[i]);
            versions.add(parent);
        }

        versions.add(extractVersion(rs));// final version

        return (Version[]) versions.toArray(new Version[0]);
    }

    private Version fetchVersion(int datasetId, int version) throws SQLException {
        ResultSet rs = queryVersion(datasetId, version);
        if (!rs.next())
            return null;

        return extractVersion(rs);
    }

    private ResultSet queryVersion(int datasetId, int version) throws SQLException {
        DataQuery query = datasource.query();
        ResultSet rs = query.executeQuery("SELECT * FROM " + datasource.getName() + ".versions WHERE dataset_id = "
                + datasetId + " AND version = " + version);

        return rs;
    }

    private Version extractVersion(ResultSet rs) throws SQLException {
        Version version = new Version();
        version.setDatasetId(rs.getInt(1));
        version.setVersion(rs.getInt(2));
        version.setPath(rs.getString("path"));
        version.setAsFinal();

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

    public Version insertVersion(Version base) throws SQLException {
        Version version = new Version();
        int newVersionNum = getNextVersionNumber(base.getDatasetId());
        version.setVersion(newVersionNum);

        version.setPath(path(base));
        version.setDatasetId(base.getDatasetId());

        insertStatement.setInt(1, base.getDatasetId());
        insertStatement.setInt(2, newVersionNum);
        insertStatement.setString(3, version.getPath());
        insertStatement.executeUpdate();

        return version;
    }

    public Version insertFinalVersion(Version base) throws SQLException {
        Version version = new Version();
        int newVersionNum = getNextVersionNumber(base.getDatasetId());
        version.setVersion(newVersionNum);

        version.setPath(path(base));
        version.setDatasetId(base.getDatasetId());
        version.setAsFinal();

        insertFinalStatement.setInt(1, base.getDatasetId());
        insertFinalStatement.setInt(2, newVersionNum);
        insertFinalStatement.setString(3, version.getPath());
        insertFinalStatement.executeUpdate();

        return version;
    }

    private String path(Version base) {
        String path = base.getPath().length() == 0 ? (base.getVersion() + "") : (base.getPath() + "," + base
                .getVersion());
        return path;
    }

    private int getNextVersionNumber(int datasetId) throws SQLException {
        nextVersionStatement.setInt(1, datasetId);
        ResultSet rs = nextVersionStatement.executeQuery();
        rs.last();

        return rs.getInt("version") + 1;
    }

    public void close() throws SQLException {
        insertStatement.close();
        nextVersionStatement.close();
    }

}
