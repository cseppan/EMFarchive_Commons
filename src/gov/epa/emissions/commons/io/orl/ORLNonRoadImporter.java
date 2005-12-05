package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.VersionedTableFormatWithOptionalCols;

import java.io.File;

public class ORLNonRoadImporter implements Importer {

    private ORLImporter delegate;
    
    public ORLNonRoadImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        FileFormatWithOptionalCols fileFormat = new ORLNonRoadFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat = new VersionedTableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols formatUnit = new DatasetTypeUnitWithOptionalCols(tableFormat, fileFormat);
        
        delegate = new ORLImporter(dataset, formatUnit, datasource, sqlDataTypes);
        delegate.setup(file);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
