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
    
    private SqlDataTypes sqlDataTypes;
        
	public ORLNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes =sqlDataTypes;
        delegate = new ORLImporter(datasource);
	}

	public void preCondition(File folder, String filePattern) throws Exception {
		delegate.preCondition(folder, filePattern);
        log.debug("folder= " + folder + " Filename= " + filePattern);
    }

	public void run(Dataset dataset) throws ImporterException {
        log.debug("Dataset Name = " +dataset.getName());

        FileFormatWithOptionalCols fileFormat = new ORLNonPointFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);

        delegate.run(dataset,unit);
        log.debug("-- END --");

    }
}
