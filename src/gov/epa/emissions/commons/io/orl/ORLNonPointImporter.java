package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ORLNonPointImporter implements Importer {
    private static Log log = LogFactory.getLog(ORLNonPointImporter.class);

    private ORLImporter delegate;

    public ORLNonPointImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        FileFormatWithOptionalCols fileFormat = new ORLNonPointFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols formatUnit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);

        delegate = new ORLImporter(dataset, formatUnit, datasource);
    }

    public void preCondition(File folder, String filePattern) throws Exception {
        delegate.preCondition(folder, filePattern);
    }

    public void run(Dataset dataset) throws ImporterException {
        log.debug("Dataset Name = " + dataset.getName());
        delegate.run();
        log.debug("-- END --");
    }
}
