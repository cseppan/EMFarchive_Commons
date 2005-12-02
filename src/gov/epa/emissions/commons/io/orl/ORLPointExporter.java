package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.io.File;

public class ORLPointExporter implements Exporter {

    private ORLExporter delegate;

    public ORLPointExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        FileFormat colsMetadata = new ORLPointFileFormat(sqlDataTypes);
        delegate = new ORLExporter(dataset, datasource, colsMetadata);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

    public void export(int version, File file) throws ExporterException {
        delegate.export(version, file);
    }

}
