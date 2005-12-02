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

    private PreparedStatement updateStatement;

    public Versions(Datasource datasource) throws SQLException {
        this.datasource = datasource;

        Connection connection = datasource.getConnection();

        String insert = "INSERT INTO " + datasource.getName() + ".versions (dataset_id,version,path) VALUES (?,?,?)";
        insertStatement = connection.prepareStatement(insert);

        String update = "UPDATE " + datasource.getName() + ".versions set final_version=true WHERE dataset_id=?";
        updateStatement = connection.prepareStatement(update);

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
            Version parent = get(datasetId, parentVersions[i]);
            versions.add(parent);
        }

        versions.add(extractVersion(rs));

        return (Version[]) versions.toArray(new Version[0]);
    }

    public Version get(int datasetId, int version) throws SQLException {
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
        if(rs.getBoolean("final_version"))
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

    public Version derive(Version base) throws SQLException {
        Version version = new Version();
        int newVersionNum = getNextVersionNumber(base.getDatasetId());

        version.setVersion(newVersionNum);
        version.setPath(path(base));
        version.setDatasetId(base.getDatasetId());

        insertStatement.setInt(1, version.getDatasetId());
        insertStatement.setInt(2, version.getVersion());
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
        version.markFinal();

        insertFinalStatement.setInt(1, base.getDatasetId());
        insertFinalStatement.setInt(2, newVersionNum);
        insertFinalStatement.setString(3, version.getPath());
        insertFinalStatement.executeUpdate();

        return version;
    }

    public Version markFinal(Version derived) throws SQLException {
        derived.markFinal();
        updateStatement.setInt(1, derived.getDatasetId());
        updateStatement.executeUpdate();

        return derived;
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
        insertFinalStatement.close();
        updateStatement.close();
    }

}
