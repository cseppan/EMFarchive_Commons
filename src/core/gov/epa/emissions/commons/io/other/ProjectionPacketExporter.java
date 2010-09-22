package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.StringTokenizer;

public class ProjectionPacketExporter extends GenericExporter {

    public ProjectionPacketExporter(Dataset dataset, String rowFilters, DbServer dbServer, Integer optimizedBatchSize) {
        super(dataset, rowFilters, dbServer, new ProjectionPacketFileFormat(dbServer.getSqlDataTypes()), optimizedBatchSize);
    }

    public ProjectionPacketExporter(Dataset dataset, String rowFilters, DbServer dbServer,DataFormatFactory factory,
            Integer optimizedBatchSize) {
        super(dataset, rowFilters, dbServer, new ProjectionPacketFileFormat(dbServer.getSqlDataTypes()), factory, optimizedBatchSize);
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) throws SQLException {
        String header = dataset.getDescription();

        if (header != null && !header.trim().isEmpty()) {
            StringTokenizer st = new StringTokenizer(header, "#");

            while (st.hasMoreTokens()) {
                String lasttoken = st.nextToken();
                if (lasttoken != null && lasttoken.trim().startsWith("/PROJECTION"))
                    writer.print(lasttoken);
                else if (lasttoken != null) {
                    String temp = lasttoken.replace("/END/", "");

                    if (!temp.trim().isEmpty())
                        writer.print("#" + temp);
                }
            }

            printExportInfo(writer);
        }
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments)
            throws Exception {
        super.writeData(writer, dataset, datasource, comments);
        writer.println("/END/");
    }

}
