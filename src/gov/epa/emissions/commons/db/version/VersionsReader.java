package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntList;

public class VersionsReader {

    private Datasource datasource;

    public VersionsReader(Datasource datasource) {
        this.datasource = datasource;
    }

    public Version[] fetchSequence(int datasetId, int finalVersion) throws SQLException {
        ResultSet rs = queryVersion(datasetId, finalVersion);
        if (!rs.next())
            return new Version[0];

        return doFetchSequence(datasetId, rs);
    }

    private Version[] doFetchSequence(int datasetId, ResultSet rs) throws SQLException {
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
}
