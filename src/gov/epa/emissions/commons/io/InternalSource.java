package gov.epa.emissions.commons.io;

import java.io.Serializable;
import java.util.regex.Pattern;

public class InternalSource implements Serializable {

	private long listindex;

	private String source;

	private String table;

	private String type;

	private String[] cols;

	private long sourceSize;

	/**
	 * @return Returns the columns.
	 */
	public String getColsList() {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < cols.length; i++) {
			buf.append(cols[i]);
			if ((i + 1) < cols.length)
				buf.append(", ");
		}

		return buf.toString();
	}

	/**
	 * @param colsList
	 *            The columns to set.
	 */
	public void setColsList(String colsList) {
		Pattern p = Pattern.compile(", ");
		cols = p.split(colsList);
	}

	public String getTable() {
		return table;
	}

	public String getType() {
		return type;
	}

	public String[] getCols() {
		return cols;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setCols(String[] cols) {
		this.cols = cols;
	}

	public long getSourceSize() {
		return sourceSize;
	}

	public void setSourceSize(long sourceSize) {
		this.sourceSize = sourceSize;
	}

	/**
	 * @return Returns the listindex.
	 */
	public long getListindex() {
		return listindex;
	}

	/**
	 * @param listindex
	 *            The listindex to set.
	 */
	public void setListindex(long listindex) {
		this.listindex = listindex;
	}

}
