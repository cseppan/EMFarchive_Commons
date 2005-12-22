package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.temporal.TableFormat;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

import java.io.File;

public class ORLOnRoadImporter implements Importer {

    private ORLImporter delegate;

    public ORLOnRoadImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        FileFormatWithOptionalCols fileFormat = new ORLOnRoadFileFormat(sqlDataTypes);
        TableFormat tableFormat = new VersionedTableFormat(fileFormat, sqlDataTypes);
        DatasetTypeUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);

        delegate = new ORLImporter(dataset, formatUnit, datasource);
        delegate.setup(file);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
