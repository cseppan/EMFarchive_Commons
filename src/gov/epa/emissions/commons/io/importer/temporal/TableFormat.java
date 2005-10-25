package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.io.Column;

public interface TableFormat {

	public abstract String key();

	public abstract String identify();

	public abstract Column[] cols();

}