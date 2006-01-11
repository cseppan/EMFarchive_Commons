package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;

import java.sql.SQLException;
import java.util.StringTokenizer;

import org.hibernate.Session;

public class NewDefaultVersionedRecordsReader implements NewVersionedRecordsReader {

    private Datasource datasource;

    private LockableVersions versions;

    public NewDefaultVersionedRecordsReader(Datasource datasource) {
        this.datasource = datasource;
        versions = new LockableVersions();
    }

    public VersionedRecord[] fetchAll(Version version, String table, Session session) throws SQLException {
        return fetchAll(version, table, null, session);
    }

    public VersionedRecord[] fetchAll(Version version, String table, String sortOrder, Session session)
            throws SQLException {
        return fetch(version, table, sortOrder, session).all();
    }

    public ScrollableVersionedRecords fetch(Version version, String table, Session session) throws SQLException {
        return fetch(version, table, null, session);
    }

    public ScrollableVersionedRecords fetch(Version version, String table, String sortOrder, Session session)
            throws SQLException {
        String versions = fetchCommaSeparatedVersionSequence(version, session);
        String deleteClause = createDeleteClause(versions);

        // FIXME: FOR BETA DEPLOYMENT ONLY: The sort is ordered by the first data column
        // After Beta the sort will have to use the sort order utility mapping
        Column sortColumn = datasource.dataModifier().getColumns(table)[5];
        sortOrder = sortColumn.name();

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

    private String fetchCommaSeparatedVersionSequence(Version finalVersion, Session session) {
        Version[] path = versions.getPath(finalVersion.getDatasetId(), finalVersion.getVersion(), session);

        StringBuffer result = new StringBuffer();
        for (int i = 0; i < path.length; i++) {
            result.append(path[i].getVersion());
            if ((i + 1) < path.length)
                result.append(",");
        }
        return result.toString();
    }

}
