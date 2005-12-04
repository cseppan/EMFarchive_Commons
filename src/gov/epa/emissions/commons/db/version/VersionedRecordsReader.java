package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

//FIXME: modify it to be encapsulated within ScrollableVersionedRecords
public class VersionedRecordsReader {

    private Datasource datasource;

    private Versions versions;

    public VersionedRecordsReader(Datasource datasource) throws SQLException {
        this.datasource = datasource;
        versions = new Versions(datasource);
    }

    public void close() throws SQLException {
        versions.close();
    }

    public VersionedRecord[] fetch(Version version, String table) throws SQLException {
        DataQuery query = datasource.query();

        String versions = fetchCommaSeparatedVersionSequence(version);
        String deleteClause = createDeleteClause(versions);

        String queryString = "SELECT * FROM " + datasource.getName() + "." + table + " WHERE dataset_id = "
                + version.getDatasetId() + " AND version IN (" + versions + ") AND " + deleteClause + " "
                + "ORDER BY version, record_id";
        ResultSet rs = query.executeQuery(queryString);

        return doFetch(rs);
    } // TODO: how does ScrollableRecords fit in here?

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

    private VersionedRecord[] doFetch(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        int columns = metadata.getColumnCount();

        List records = new ArrayList();

        while (rs.next()) {
            VersionedRecord record = new VersionedRecord();
            record.setRecordId(rs.getInt("record_id"));
            record.setDatasetId(rs.getInt("dataset_id"));
            record.setVersion(rs.getInt("version"));
            record.setDeleteVersions(rs.getString("delete_versions"));

            for (int i = 5; i <= columns; i++)
                record.add(rs.getString(i));
            records.add(record);
        }

        return (VersionedRecord[]) records.toArray(new VersionedRecord[0]);
    }

}
