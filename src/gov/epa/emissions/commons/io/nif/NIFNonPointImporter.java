package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class NIFNonPointImporter {

	private NIFImporter delegate;
	
	private SqlDataTypes sqlDataTypes;

	public NIFNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
		this.sqlDataTypes = sqlDataTypes;
		delegate = new NIFImporter(datasource, sqlDataTypes);
	}

	private void preImport(Dataset dataset) throws ImporterException {
		List internalSources = Arrays.asList(dataset.getInternalSources());
		try {
			for (int i = 0; i < internalSources.size(); i++) {
				InternalSource source = (InternalSource) internalSources.get(i);
				File file = new File(source.getSource());
				FileFormat colMetadata = createColumnMetaData(file, sqlDataTypes);
				
			}
		} catch (Exception e) {
			throw new ImporterException("Failed to import-" + e.getMessage());
		}

	}

	private FileFormat createColumnMetaData(File file, SqlDataTypes sqlDataTypes) throws IOException, ImporterException {
		String notation = notation(file);
		if(notation ==null){
			return null;
		}
		notation = notation.toLowerCase();
		if(notation.equals("ce"))
			return new NIFAreaControlEfficiencyFileFormat(sqlDataTypes);
		if(notation.equals("pe"))
			return new NIFAreaPeriodsFileFormat(sqlDataTypes);
		if(notation.equals("em"))
			return new NIFAreaEmissionFileFormat(sqlDataTypes);
		if(notation.equals("ep"))
			return new NIFAreaEmissionsProcessFileFormat(sqlDataTypes);
		throw new ImporterException("The notation '"+notation +"' does not match possible notations (ce, pe, em, ep)");
	}

	private String notation(File file) throws FileNotFoundException,
			IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line = null;
		String fileType = null;
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			//first 2chars from a line which is  not comment
			if (line.length() > 1 && line.charAt(0) != '#') {
				return line.substring(0, 2);
			}
		}
		reader.close();
		return null;
	}

	public void run(Dataset dataset) throws ImporterException {
		preImport(dataset);
		delegate.run(dataset);
	}

}
