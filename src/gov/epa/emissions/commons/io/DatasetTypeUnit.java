package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

public class DatasetTypeUnit {

	private TableFormat tableMetadata;

	private FileFormat fileMetadata;

	private boolean required;

	public DatasetTypeUnit(TableFormat tableMetadata, FileFormat fileMetadata,
			boolean required) {
		this.tableMetadata = tableMetadata;
		this.fileMetadata = fileMetadata;
		this.required = required;
	}

	public boolean isRequired() {
		return required;
	}

	public FileFormat getFileMetadata() {
		return fileMetadata;
	}

	public TableFormat getTableMetadata() {
		return tableMetadata;
	}

}
