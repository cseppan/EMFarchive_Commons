package gov.epa.emissions.commons.io.importer.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.FileColumnsMetadata;
import gov.epa.emissions.commons.io.importer.FormattedImporter;
import gov.epa.emissions.commons.io.importer.TableType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LineImporter extends FormattedImporter {
	
	private static Log log = LogFactory.getLog(LineImporter.class);
	private final String[] basetypes = {"TextLines"};
	private final String summary = "TextLinesSummary";
	
	protected LineImporter(DbServer dbServer) {
		super(dbServer);
		// TODO Auto-generated constructor stub
	}
	
    /**
     * Take a array of Files and put them database, overwriting existing
     * corresponding tables specified in dataset based on overwrite flag.
     * 
     * @param files -
     *            an array of Files which are checked prior to import
     * @param dataset -
     *            Dataset specifying needed properties such as datasetType and
     *            table name (table name look-up is based on file name)
     * @param overwrite -
     *            whether or not to overwrite corresponding tables
     */
    public void run(File[] files, Dataset dataset, boolean overwrite) throws Exception {
        this.dataset = dataset;

        // explicitly make sure only one valid file is returned
        if (files.length != 1) {
            throw new Exception("Can only import one valid input file at a time: " + files);
        }
  
        // import the file
        Datasource datasource = dbServer.getEmissionsDatasource();
        importFile(files[0], datasource, overwrite);
    }

    /**
     * import a single file into the specified database
     * 
     * @param file -
     *            the file to be ingested in
     * @param dbName -
     *            the database into which the data is ingested from the file
     * @param details -
     *            the details with which to import the file
     */
    public void importFile(File file, Datasource datasource, boolean overwrite) throws Exception {
        // get a bufferedreader for the file to be imported in
        BufferedReader reader = new BufferedReader(new FileReader(file));

        long fileLength = file.length() + 1;
        long lastModified = file.lastModified();
        // if file is small enough, mark file read ahead limit from beginning
        // so we can come back without having to close and reopen the file
        if (fileLength < GenericNames.READ_AHEAD_LIMIT) {
            reader.mark((int) fileLength);
        }

        // if able, go back to the file beginning
        if (fileLength < GenericNames.READ_AHEAD_LIMIT) {
            reader.reset();
        }
        // else close, reopen and check for modification
        else {
            // close the file
            reader.close();
            // reopen the file
            reader = new BufferedReader(new FileReader(file));
            // check the file for modification
            long currentLastModified = file.lastModified();
            if (lastModified != currentLastModified) {
                reader.close();
                throw new Exception("File " + file.getAbsolutePath()
                        + " changed during import. Do not edit file while import is executing.");
            }
        }

        FileColumnsMetadata metadata = getFileColumnsMetadata();
        String[] columnNames = metadata.getColumnNames();
        String[] columnTypes = metadata.getColumnTypes();
        int[] columnWidths = metadata.getColumnWidths();

        doImport(file, datasource, reader, columnNames, columnTypes, columnWidths, overwrite);
    }

    private String doImport(File file, Datasource datasource, BufferedReader reader, String[] columnNames,
            String[] columnTypes, int[] columnWidths, boolean overwrite) throws Exception {
        String fileName = file.getName();
        DatasetType datasetType = dataset.getDatasetType();
        TableType tableType = new TableType(datasetType, basetypes, summary);
        if (tableType == null) {
            throw new Exception("Could not determine table type for file name: " + fileName);
        }

        // use the table type to get the table name
        String baseTableType = tableType.baseTypes()[0];
        Table table = dataset.getTable(baseTableType);
        String tableName = table.getName().trim();

        if (tableName == null) {
            throw new Exception("The dataset did not specify the table name for file name: " + fileName);
        } else if (tableName.length() == 0) {
            throw new Exception("The table name must be at least one character long for file name: " + fileName);
        }

        TableDefinition tableDefinition = datasource.tableDefinition();
        if (overwrite) {
            tableDefinition.deleteTable(tableName);
        }
        // else make sure table does not exist
        else if (tableDefinition.tableExists(tableName)) {
            log.error("The table \"" + tableName
                    + "\" already exists. Please select 'overwrite tables if exist' or choose a new table name.");
            throw new Exception("The table \"" + tableName
                    + "\" already exists. Please select 'overwrite tables if exist' or choose a new table name.");
        }

        tableDefinition.createTable(tableName, columnNames, columnTypes, null);
        String line = null;
        Vector v = new Vector();
        int numRows = 0;

        // read lines in one at a time and put the data into database.. this
        // will avoid huge memory consumption
        while ((line = reader.readLine()) != null) {
            // skip over non data lines and those too long lines as needed
        	int len = line.trim().length();
            if (len < columnWidths[0] && len > 0) {
                v.add(line);
                numRows++;
            }
        }// while file is not empty
        
        String[] data = new String[v.size()];
        v.copyInto(data);
        datasource.getDataModifier().insertRow(tableName, data, columnTypes);

        // perform capable table type specific processing
        postProcess(datasource, tableName, baseTableType);

        // when all the data is done ingesting..
        // close the database connections by calling acceptor.finish..
        // and close the reader & writer as well..
        reader.close();

        return tableName;
    }
    
    // TODO: pull this out into a factory
    private FileColumnsMetadata getFileColumnsMetadata() throws Exception {
    	FileColumnsMetadata details = new FileColumnsMetadata("", dbServer.getDataType());
        
    	/**
    	 * Need to setup a generic DataFormat
    	 */
    	
    	details.addColumnName(GenericNames.COLUMN_NAME);
        try {
            details.setType(GenericNames.COLUMN_NAME, GenericNames.COLUMN_TYPE);
            details.setWidth(GenericNames.COLUMN_NAME, String.valueOf(GenericNames.COLUMN_WIDTH));
        } catch (Exception e) {
        }
        
        return details;
    }

	protected String[] breakUpLine(String line, int[] widths) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
