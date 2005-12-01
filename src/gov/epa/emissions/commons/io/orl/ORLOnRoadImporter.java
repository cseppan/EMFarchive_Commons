package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.SimpleTableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

import java.io.File;

public class ORLOnRoadImporter implements Importer {

    private ORLImporter delegate;

    public ORLOnRoadImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        FileFormatWithOptionalCols fileFormat = new ORLOnRoadFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat = new SimpleTableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols formatUnit = new DatasetTypeUnitWithOptionalCols(tableFormat, fileFormat);

        delegate = new ORLImporter(dataset, formatUnit, datasource);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run();
    }

    public void preCondition(File folder, String filePattern) throws Exception {
        delegate.preCondition(folder, filePattern);
    }

}
