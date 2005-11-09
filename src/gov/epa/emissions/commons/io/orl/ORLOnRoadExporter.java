package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.io.File;

public class ORLOnRoadExporter implements Exporter {

    private ORLExporter delegate;

    public ORLOnRoadExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        FileFormat colsMetadata = new ORLOnRoadFileFormat(sqlDataTypes);
        delegate = new ORLExporter(dataset, datasource, colsMetadata);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

}
