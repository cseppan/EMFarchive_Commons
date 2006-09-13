package gov.epa.emissions.commons.io;

import java.util.StringTokenizer;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.db.version.Version;

public class VersionedDatasetQuery implements ExportStatement {

    private Version version;

    private DatasetType datasetType;

    public VersionedDatasetQuery(Version version, Dataset dataset) {
        this.version = version;
        this.datasetType = dataset.getDatasetType();
    }

    public String generate(String table) {
        String versionsPath = version.createCompletePath();
        String deleteClause = createDeleteClause(versionsPath);

        return "SELECT * FROM " + table + " AS a WHERE version IN (" + versionsPath + ") AND " + deleteClause
                + " AND a.dataset_id=" + version.getDatasetId() + orderByClause();
    }

    private String orderByClause() {
        String defaultSortOrder = datasetType.getDefaultSortOrder();
        if (defaultSortOrder != null) {
            defaultSortOrder = defaultSortOrder.trim();
            if (defaultSortOrder.length() > 0)
                return " ORDER BY " + defaultSortOrder;
        }

        return "";
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
}
