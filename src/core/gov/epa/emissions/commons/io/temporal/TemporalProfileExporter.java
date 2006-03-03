package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.other.CountryStateCountyDataExporter;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TemporalProfileExporter extends CountryStateCountyDataExporter {
    public TemporalProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        this(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public TemporalProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, sqlDataTypes, dataFormatFactory);
    }
    
    protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource[] sources = dataset.getInternalSources();

        for(int i = 0; i < sources.length; i++){
            ResultSet data = getResultSet(sources[i], q);
            String[] cols = getCols(data);
            writer.println("/" + sources[i].getTable() + "/");

            while (data.next())
                writeRecordWithComments(cols, data, writer);
            
            writer.println("/END/");
        }
    }
    
    protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource[] sources = dataset.getInternalSources();

        for(int i = 0; i < sources.length; i++){
            ResultSet data = getResultSet(sources[i], q);
            String[] cols = getCols(data);
            writer.println("/" + sources[i].getTable() + "/");

            while (data.next())
                writeRecordWithoutComments(cols, data, writer);
            
            writer.println("/END/");
        }
    }

}
