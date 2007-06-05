package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ORLExporter extends GenericExporter {

    private Dataset dataset;

    private Datasource datasource;

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, DataFormatFactory dataFormatFactory,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, fileFormat, dataFormatFactory, optimizedBatchSize);
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        setDelimiter(",");
    }

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, Integer optimizeBatchSize) {
        this(dataset, dbServer, fileFormat, new NonVersionedDataFormatFactory(), optimizeBatchSize);
    }

    public void export(File file) throws ExporterException {
        try {
            file.createNewFile();
            file.setWritable(true, false);
        } catch (IOException e) {
            throw new ExporterException("Could not create export file: " + file.getAbsolutePath());
        }
        
        String query = getQueryString(dataset, datasource);
        
        try {
            String writeQuery = "COPY (" + query + ") to ' " + file.getCanonicalPath() + "' WITH CSV HEADER QUOTE '\"'";
           System.out.println(writeQuery);
            Connection connection = datasource.getConnection();
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.executeQuery(writeQuery);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ExporterException("Could not connect to db server: " + e.getMessage());
        }
    }

}
