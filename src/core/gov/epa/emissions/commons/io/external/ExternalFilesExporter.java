package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.ExternalSource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.File;

public class ExternalFilesExporter implements Exporter {

    private Dataset dataset;
    
    private int count = 0;

    public ExternalFilesExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizedBatchSize) {
        this(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory(), optimizedBatchSize);
    }

    public ExternalFilesExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory, Integer optimizedBatchSize) {
        this.dataset = dataset;
    }

    public void export(File file) throws ExporterException {
        ExternalSource[] srcs = dataset.getExternalSources();
        verifyExistance(srcs);
    }

    private void verifyExistance(ExternalSource[] srcs) throws ExporterException {
        for (int i = 0; i < srcs.length; i++) {
            String fileName = srcs[i].getDatasource();
            
            if (!new File(fileName).exists())
                throw new ExporterException("The file " + fileName + " doesn't exist.");
            
            this.count = i + 1;
        }
        
    }

    public long getExportedLinesCount() {
        return count; // index starts 0
    }

}