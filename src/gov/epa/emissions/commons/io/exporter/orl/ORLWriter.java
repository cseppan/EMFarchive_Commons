package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ORLWriter {

    private ORLHeaderWriter headerWriter;

    private ORLBodyFactory bodyFactory;

    private DbServer dbServer;

    public ORLWriter(DbServer dbServer, ORLDatasetTypesFactory typesFactory) {
        this.dbServer = dbServer;
        this.headerWriter = new ORLHeaderWriter(typesFactory);
        this.bodyFactory = new ORLBodyFactory(typesFactory);
    }

    public void write(Dataset dataset, File file) throws Exception {
        PrintWriter writer = null;

        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file.getCanonicalPath())));
            writeHeader(dataset, writer);
            writeBody(dataset, writer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("could not export file - " + file, e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    private void writeBody(Dataset dataset, PrintWriter writer) throws SQLException {
        Datasource datasource = dbServer.getEmissionsDatasource();

        // TODO: we know ORL only has a single base table, but cleaner
        // interface needed
        Table baseTable = dataset.getTables()[0];
        String qualifiedTableName = datasource.getName() + "." + baseTable.getName();

        DataQuery query = datasource.query();
        ResultSet data = query.selectAll(qualifiedTableName);

        ORLBody body = bodyFactory.getBody(dataset.getDatasetType());
        body.write(data, writer);
    }

    private void writeHeader(Dataset dataset, PrintWriter writer) {
        headerWriter.writeHeader(dataset, writer);
    }
}
