package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.other.SMKReportExporter;

import java.io.File;

public class CSVExporter implements Exporter {
    
    private SMKReportExporter delegate;
    
    public CSVExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        setup(dataset, datasource, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public CSVExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory) {
        setup(dataset, datasource, sqlDataTypes, formatFactory);
    }
    
    private void setup(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory factory){
        this.delegate = new SMKReportExporter(dataset, datasource, types, factory);
    }
    
    public void export(File file) throws ExporterException {
        delegate.export(file);
    }
}
