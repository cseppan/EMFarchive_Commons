/*
 * Creation on Oct 13, 2005
 * Eclipse Project Name: EMF
 * File Name: InternalSource.java
 * Author: Conrad F. D'Cruz
 */
/**
 * 
 */

package gov.epa.emissions.commons.io;

import java.io.Serializable;

/**
 * @author Conrad F. D'Cruz
 *
 */
public class ExternalSource implements Serializable {

	private String datasource;
	
	/**
	 * 
	 */
	public ExternalSource() {
		super();
	}

	/**
	 * @param datasource
	 */
	public ExternalSource(String datasource) {
		super();
		// TODO Auto-generated constructor stub
		this.datasource = datasource;
	}

	/**
	 * @return Returns the datasource.
	 */
	public String getDatasource() {
		return datasource;
	}

	/**
	 * @param datasource The datasource to set.
	 */
	public void setDatasource(String datasource) {
		this.datasource = datasource;
	}

}
