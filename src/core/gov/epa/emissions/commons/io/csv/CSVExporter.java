package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.other.SMKReportExporter;

import java.io.File;

public class CSVExporter implements Exporter {
    
    private SMKReportExporter delegate;
    
    public CSVExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizedBatchSize) {
        setup(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory(),optimizedBatchSize);
    }

    public CSVExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory, Integer optimizedBatchSize) {
        setup(dataset, dbServer, sqlDataTypes, formatFactory,optimizedBatchSize);
    }
    
    private void setup(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize){
        this.delegate = new SMKReportExporter(dataset, dbServer, types, optimizedBatchSize);
        this.delegate.setDelimiter(",");
    }
    
    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

}
