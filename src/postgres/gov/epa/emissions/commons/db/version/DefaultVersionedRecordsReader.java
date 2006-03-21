package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;

import java.sql.SQLException;
import java.util.StringTokenizer;

import org.hibernate.Session;

public class DefaultVersionedRecordsReader implements VersionedRecordsReader {
    private Datasource datasource;

    private Versions versions;

    public DefaultVersionedRecordsReader(Datasource datasource) {
        this.datasource = datasource;
        versions = new Versions();
    }

    public ScrollableVersionedRecords fetch(Version version, String table, Session session) throws SQLException {
        return fetch(version, table, null, null, null, session);
    }

    public ScrollableVersionedRecords fetch(Version version, String table, String columnFilter, String rowFilter,
            String sortOrder, Session session) throws SQLException {
        String query = createQuery(version, table, columnFilter, rowFilter, sortOrder, session);
        ScrollableVersionedRecords records = new DefaultScrollableVersionedRecords(datasource, query);
        records.execute();

        return records;
    }

    VersionedRecord[] fetchAll(Version version, String table, Session session) throws SQLException {
        return fetchAll(version, table, null, null, null, session);
    }

    VersionedRecord[] fetchAll(Version version, String table, String columnFilter, String rowFilter, String sortOrder,
            Session session) throws SQLException {
        return fetch(version, table, columnFilter, rowFilter, sortOrder, session).all();
    }

    private String createQuery(Version version, String table, String columnFilter, String rowFilter, String sortOrder,
            Session session) {

        String versions = fetchCommaSeparatedVersionSequence(version, session);

        String columnFilterClause = columnFilterClause(columnFilter);
        String rowFilterClause = rowFilterClause(version, rowFilter, versions);
        String sortOrderClause = sortOrderClause(sortOrder);

        String extraWhereClause = "";
        extraWhereClause = extraWhereClause + "";

        String query = "SELECT " + columnFilterClause + " FROM " + datasource.getName() + "." + table + rowFilterClause
                + " ORDER BY " + sortOrderClause;
        return query;
    }

    private String sortOrderClause(String sortOrder) {
        final String defaultSortOrderClause = "record_id";
        String sortOrderClause = defaultSortOrderClause;
        if ((sortOrder != null) && (sortOrder.length() > 0)) {
            sortOrderClause = sortOrder + "," + defaultSortOrderClause;
        }

        return sortOrderClause;
    }

    private String columnFilterClause(String columnFilter) {
        final String defaultColumnFilterClause = "*";
        String columnFilterClause = defaultColumnFilterClause;
        if ((columnFilter != null) && (columnFilter.length() > 0)) {
            columnFilterClause = columnFilter + ", " + "record_id, dataset_id, version, delete_versions";
        }

        return columnFilterClause;
    }

    private String rowFilterClause(Version version, String rowFilter, String versions) {
        String deleteClause = createDeleteClause(versions);

        String defaultRowFilterClause = " WHERE dataset_id = " + version.getDatasetId() + " AND version IN ("
                + versions + ") AND " + deleteClause;
        String rowFilterClause = defaultRowFilterClause;
        if ((rowFilter != null) && (rowFilter.length() > 0)) {
            rowFilterClause = defaultRowFilterClause + " AND " + rowFilter;
        }
        return rowFilterClause;
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
