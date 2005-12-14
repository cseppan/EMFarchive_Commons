package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.SQLException;
import java.util.StringTokenizer;

public class DefaultVersionedRecordsReader implements VersionedRecordsReader {

    private Datasource datasource;

    private Versions versions;

    public DefaultVersionedRecordsReader(Datasource datasource) throws SQLException {
        this.datasource = datasource;
        versions = new Versions(datasource);
    }

    public void close() throws SQLException {
        versions.close();
    }

    public VersionedRecord[] fetchAll(Version version, String table) throws SQLException {
        return fetchAll(version, table, null);
    }

    public VersionedRecord[] fetchAll(Version version, String table, String sortOrder) throws SQLException {
        return fetch(version, table, sortOrder).all();
    }

    public ScrollableVersionedRecords fetch(Version version, String table) throws SQLException {
        return fetch(version, table, null);
    }

    public ScrollableVersionedRecords fetch(Version version, String table, String sortOrder) throws SQLException {
        String versions = fetchCommaSeparatedVersionSequence(version);
        String deleteClause = createDeleteClause(versions);

        String queryString = "SELECT * FROM " + datasource.getName() + "." + table + " WHERE dataset_id = "
                + version.getDatasetId() + " AND version IN (" + versions + ") AND " + deleteClause + " "
                + "ORDER BY version, record_id";

        if (sortOrder != null)
            queryString += "," + sortOrder;

        ScrollableVersionedRecords records = new DefaultScrollableVersionedRecords(datasource, queryString);
        records.execute();

        return records;
    }

    private String createDeleteClause(String versions) {
        StringBuffer buffer = new StringBuffer();

        StringTokenizer tokenizer = new StringTokenizer(versions, ",");
        // e.g.: delete_version NOT SIMILAR TO '(6|6,%|%,6,%|%,6)'
        while (tokenizer.hasMoreTokens()) {
            String version = tokenizer.nextToken();
            String regex = "(" + version + "|" + version + ",%|%," + version + ",%|%," + version + ")";
            buffer.append(" delete_versions NOT SIMILAR TO '" + regex + "'");

            if (tokenizer.hasMoreTokens())
                buffer.append(" AND ");
        }

        return buffer.toString();
    }

    private String fetchCommaSeparatedVersionSequence(Version finalVersion) throws SQLException {
        Version[] path = versions.getPath(finalVersion.getDatasetId(), finalVersion.getVersion());

        StringBuffer result = new StringBuffer();
        for (int i = 0; i < path.length; i++) {
            result.append(path[i].getVersion());
            if ((i + 1) < path.length)
                result.append(",");
        }
        return result.toString();
    }

}
