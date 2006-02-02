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

    public VersionedRecord[] fetchAll(Version version, String table, Session session) throws SQLException {
        return fetchAll(version, table, null, null, null, session);
    }

    public VersionedRecord[] fetchAll(Version version, String table, String columnFilter, String rowFilter, String sortOrder, Session session)
            throws SQLException {
        return fetch(version, table, columnFilter, rowFilter, sortOrder, session).all();
    }

    public ScrollableVersionedRecords fetch(Version version, String table, Session session) throws SQLException {
        return fetch(version, table, null, null, null, session);
    }

    public ScrollableVersionedRecords fetch(Version version, String table, String columnFilter, String rowFilter,
            String sortOrder, Session session) throws SQLException {

        String versions = fetchCommaSeparatedVersionSequence(version, session);
        String deleteClause = createDeleteClause(versions);

        // these are the default clauses if the user does not specify additional sort/filter parameters
        final String defaultColumnFilterClause = "*";
        
        final String defaultSortOrderClause = "record_id";
        final String defaultRowFilterClause = " WHERE dataset_id = " + version.getDatasetId() + " AND version IN (" + versions + ") AND " + deleteClause;

        String columnFilterClause = "";
        String rowFilterClause = "";
        String sortOrderClause = "";

        if ((columnFilter == null) || (columnFilter.equals(""))) {
            columnFilterClause = defaultColumnFilterClause;
        } else {
            columnFilterClause = columnFilter + ", " + "record_id, dataset_id, version, delete_versions";
        }

        if ((rowFilter == null) || (rowFilter.equals(""))) {
            rowFilterClause = defaultRowFilterClause;
        } else {
            rowFilterClause = defaultRowFilterClause + rowFilter;
        }

        if ((sortOrder == null) || (sortOrder.equals(""))) {

            // FIXME: need the rules for choosing the number of data columns to sort by
            // Column sortColumn = datasource.dataModifier().getColumns(table)[4];
            // sortOrder = sortColumn.name();
            sortOrderClause = defaultSortOrderClause;
        } else {
            sortOrderClause = sortOrder + "," + defaultSortOrderClause;
        }

        String extraWhereClause = "";
        extraWhereClause = extraWhereClause + "";

        String queryString = "SELECT " + columnFilterClause + " FROM " + datasource.getName() + "." + table
                + rowFilterClause + " ORDER BY " + sortOrderClause;

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

    private String fetchCommaSeparatedVersionSequence(Version version, Session session) {
        Version[] path = versions.getPath(version.getDatasetId(), version.getVersion(), session);

        StringBuffer result = new StringBuffer();
        for (int i = 0; i < path.length; i++) {
            result.append(path[i].getVersion());
            if ((i + 1) < path.length)
                result.append(",");
        }
        return result.toString();
    }

}
