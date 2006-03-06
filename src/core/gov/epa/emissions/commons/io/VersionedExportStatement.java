package gov.epa.emissions.commons.io;

import java.util.StringTokenizer;

import gov.epa.emissions.commons.db.version.Version;

public class VersionedExportStatement implements ExportStatement {

    private Version version;

    public VersionedExportStatement(Version version) {
        this.version = version;
    }

    public String generate(String table) {
        String versionsPath = version.createCompletePath();
        String deleteClause = createDeleteClause(versionsPath);

        return "SELECT * FROM " + table + " WHERE version IN (" + versionsPath + ") AND " + deleteClause;
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
