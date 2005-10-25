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

public class ORLOnRoadImporter implements Importer {

    private ORLImporter delegate;

    public ORLOnRoadImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        FileFormatWithOptionalCols fileFormat = new ORLOnRoadFileFormat(sqlDataTypes);

        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);

        delegate = new ORLImporter(datasource, unit);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run(dataset);
    }

    public void preCondition(File folder, String filePattern) {
        delegate.preCondition(folder, filePattern);
    }

}
