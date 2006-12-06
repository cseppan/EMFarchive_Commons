package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAMobileExporter implements Exporter {

    private IDAExporter delegate;

    public IDAMobileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizedBatchSize) throws ImporterException {
        this.delegate = new IDAExporter(dataset, dbServer, fileFormat(sqlDataTypes), optimizedBatchSize);
    }

    public IDAMobileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) throws ImporterException {
        this.delegate = new IDAExporter(dataset, dbServer, fileFormat(sqlDataTypes), dataFormatFactory,
                optimizedBatchSize);
    }

    private IDAFileFormat fileFormat(SqlDataTypes sqlDataTypes) {
        return new IDAMobileFileFormat(sqlDataTypes);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

    public long getExportedLinesCount() {
        return delegate.getExportedLinesCount();
    }

}
