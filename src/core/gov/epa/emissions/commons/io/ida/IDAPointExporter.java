package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.File;

public class IDAPointExporter extends GenericExporter {

    private IDANonPointNonRoadExporter deligate;
    
    public IDAPointExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat) {
        super(dataset, dbServer, fileFormat);
        this.deligate = new IDANonPointNonRoadExporter(dataset, dbServer, fileFormat);
    }

    public IDAPointExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, fileFormat, dataFormatFactory);
        this.deligate = new IDANonPointNonRoadExporter(dataset, dbServer, fileFormat, dataFormatFactory);
    }
    
    public void export(File file) throws ExporterException {
        deligate.export(file);
    }

}
