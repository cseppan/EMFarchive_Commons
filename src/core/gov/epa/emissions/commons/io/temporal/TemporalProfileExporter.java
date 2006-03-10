package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.other.CountryStateCountyDataExporter;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TemporalProfileExporter extends CountryStateCountyDataExporter {
    
    private SqlDataTypes sqlDataTypes;
    
    private FileFormat fileFormat;
    
    public TemporalProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        this(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
        this.sqlDataTypes = sqlDataTypes;
    }

    public TemporalProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, sqlDataTypes, dataFormatFactory);
        this.sqlDataTypes = sqlDataTypes;
    }
    
    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource[] sources = dataset.getInternalSources();

        for(int i = 0; i < sources.length; i++){
            ResultSet data = getResultSet(sources[i], q);
            String[] cols = getCols(data);
            String sectionName = sources[i].getTable().replace('_', ' ');
            writer.println("/" + sectionName + "/");
            this.fileFormat = getFileFormat(sectionName);

            if(comments) {
                while (data.next()) 
                    writeRecordWithComments(cols, data, writer);
            } else {
                while (data.next())
                    writeRecordWithoutComments(cols, data, writer);
            }
            
            writer.println("/END/");
        }
    }
    
    protected FileFormat getFileFormat(String fileFormatName) {
        TemporalFileFormatFactory factory = new TemporalFileFormatFactory(sqlDataTypes);

        return factory.get(fileFormatName);
    }
    
    protected String formatValue(String[] cols, int index, ResultSet data) throws SQLException {
        int fileIndex = index;
        if (isTableVersioned(cols))
            fileIndex = index - 3;

        Column column = fileFormat.cols()[fileIndex - 2];
        return getFixedPositionValue(column, data);
    }

}
