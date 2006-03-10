package gov.epa.emissions.commons.io.ida;

import java.io.File;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class IDAMobileExporter extends GenericExporter {

    private IDANonPointNonRoadExporter deligate;
    
    public IDAMobileExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat) {
        super(dataset, dbServer, fileFormat);
        this.deligate = new IDANonPointNonRoadExporter(dataset, dbServer, fileFormat);
    }

    public IDAMobileExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory) {
        super(dataset, dbServer, fileFormat, dataFormatFactory);
        this.deligate = new IDANonPointNonRoadExporter(dataset, dbServer, fileFormat, dataFormatFactory);
    }
    
    public void export(File file) throws ExporterException {
        deligate.export(file);
    }

}
