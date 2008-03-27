package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.db.version.Version;

public class VersionedDatasetQuery implements ExportStatement {

    private DatasetType datasetType;

    private VersionedQuery versionedQuery;

    public VersionedDatasetQuery(Version version, Dataset dataset) {
        this.datasetType = dataset.getDatasetType();
        versionedQuery = new VersionedQuery(version);
    }

    public String generate(String table) {
        return "SELECT * FROM " + table + " WHERE " + versionedQuery.query() + orderByClause();
    }
    
    public String generateFilteringQuery(String colString, String table, String filter) {
        if (filter == null || filter.trim().isEmpty())
            filter = "";
        else
            filter = " AND " + filter;
        
        return "SELECT " + colString + " FROM " + table + " WHERE " + versionedQuery.query() + filter + orderByClause();
    }
    
    public String getVersionQuery() {
        return versionedQuery.query();
    }
    
    public String getDefaultOrderByClasue() {
        return orderByClause();
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


}
