package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

public class DatasetTypeUnitWithOptionalCols {

	private TableFormatWithOptionalCols tableMetadata;

	private FileFormatWithOptionalCols fileMetadata;

	private boolean required;

	public DatasetTypeUnitWithOptionalCols(
			TableFormatWithOptionalCols tableMetadata,
			FileFormatWithOptionalCols fileMetadata, boolean required) {
		this.tableMetadata = tableMetadata;
		this.fileMetadata = fileMetadata;
		this.required = required;
	}

	public boolean isRequired() {
		return required;
	}

	public FileFormatWithOptionalCols fileFormat() {
		return fileMetadata;
	}

	public TableFormatWithOptionalCols tableFormat() {
		return tableMetadata;
	}

}
