package gov.epa.emissions.commons.io;

import java.util.StringTokenizer;

import gov.epa.emissions.commons.db.version.Version;

public class VersionedQuery {

    private Version version;
    
    public VersionedQuery(Version version) {
        this.version = version;
    }
    
    public String query(){
        String versionsPath = version.createCompletePath();
        String deleteClause = createDeleteClause(versionsPath);

        return "version IN (" + versionsPath + ") AND " + deleteClause + " AND "+datasetIdClause();
    }
    
    private String datasetIdClause() {
        return "dataset_id=" + version.getDatasetId();
    }

    private String createDeleteClause(String versions) {
        StringBuffer buffer = new StringBuffer();

        StringTokenizer tokenizer = new StringTokenizer(versions, ",");
        // e.g.: delete_version NOT SIMILAR TO '(6|6,%|%,6,%|%,6)'
        while (tokenizer.hasMoreTokens()) {
            String version = tokenizer.nextToken();
            if (!version.equals("0"))  // don't need to check to see if items are deleted from version 0
            {
                String regex = "(" + version + "|" + version + ",%|%," + version + ",%|%," + version + ")";
                buffer.append(" delete_versions NOT SIMILAR TO '" + regex + "'");
    
                if (tokenizer.hasMoreTokens())
                    buffer.append(" AND ");
            }
        }

        return buffer.toString();
    }

}
