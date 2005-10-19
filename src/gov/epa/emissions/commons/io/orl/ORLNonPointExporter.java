package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.NewExporter;
import gov.epa.emissions.commons.io.exporter.orl.ExporterException;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

import java.io.File;

public class ORLNonPointExporter implements NewExporter {

    private NewORLExporter delegate;

    public ORLNonPointExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        ColumnsMetadata colsMetadata = new ORLNonPointColumnsMetadata(sqlDataTypes);
        delegate = new NewORLExporter(dataset, datasource, colsMetadata);
    }

    public void export(File file) throws ExporterException {
        delegate.export(file);
    }

}