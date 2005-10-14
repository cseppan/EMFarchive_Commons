package gov.epa.emissions.commons.io;

import java.io.Serializable;
import java.util.StringTokenizer;

public class InternalSource implements Serializable {

	//do not touch needed for hibernate mapping
	private long internalsourceid;
	
	private final String INTERNALSOURCE_COL_DELIMITER=",";
    private String source;
    private String table;
    private String type;
    private String[] cols;
	private String columns;
    private long sourceSize;

    /**
	 * @return Returns the columns.
	 */
	public String getColumns() {
		columns="";
		if (cols.length>0){
			for (int i=0;i<cols.length;i++){
				columns=columns + INTERNALSOURCE_COL_DELIMITER + cols[i]; 
			}
			
		}
		return columns;
	}

	/**
	 * @param columns The columns to set.
	 */
	public void setColumns(String columns) {
		this.columns = columns;
		
		StringTokenizer stk = new StringTokenizer(columns,INTERNALSOURCE_COL_DELIMITER);
		if (columns.length()>0){
			cols = new String[stk.countTokens()];
			int i=0;
			while (stk.hasMoreTokens()){
				cols[i]=stk.nextToken();
			}			
		}
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
	 * @return Returns the internalsourceid.
	 */
	public long getInternalsourceid() {
		return internalsourceid;
	}

	/**
	 * @param internalsourceid The internalsourceid to set.
	 */
	public void setInternalsourceid(long internalsourceid) {
		this.internalsourceid = internalsourceid;
	}

}
