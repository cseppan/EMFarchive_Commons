package gov.epa.emissions.commons.io.importer.ref;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.FieldDefinitionsFileReader;
import gov.epa.emissions.commons.io.importer.FileColumnsMetadata;
import gov.epa.emissions.commons.io.importer.FixedFormatImporter;
import gov.epa.emissions.commons.io.importer.TableType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class represents the ReferenceImporter for the reference database. TODO:
 * replace by injection. Combine Reference Tables & Reference Importer
 */
public class ReferenceImporter extends FixedFormatImporter {
    private File fieldDefsFile;

    private File referenceFilesDir;

    /** the field definitions file reader * */
    private FieldDefinitionsFileReader fieldDefsReader = null;

    private ReferenceTableTypes tableTypes;

    private static final String REF_DIR_NAME = "refFiles";

    public ReferenceImporter(DbServer dbServer, File fieldDefsFileName, File referenceFilesDir, boolean useTransactions) {
        super(dbServer);
        this.tableTypes = new ReferenceTableTypes();
        this.fieldDefsFile = fieldDefsFileName;
        this.referenceFilesDir = referenceFilesDir;
        this.useTransactions = useTransactions;
    }

    /**
     * Take a array of Files and put them database, overwriting existing
     * corresponding tables specified in dataset based on overwrite flag.
     */
    public void run(File[] files, Dataset dataset, boolean overwrite) throws Exception {
        super.dataset = dataset;

        files = verifyExpectedFiles(dataset.getDatasetType(), files);

        fieldDefsReader = new FieldDefinitionsFileReader(fieldDefsFile, dbServer.getDataType());

        // import each file (--> database table) one by one..
        Datasource datasource = dbServer.getReferenceDatasource();
        for (int i = 0; i < files.length; i++) {
            importFile(files[i], datasource, getDetails(files[i]), overwrite);
        }
    }

    private File[] verifyExpectedFiles(DatasetType type, File[] files) throws Exception {
        TableType tableType = tableTypes.type(type);

        // flags for when we find a file for the table type
        String[] baseTableTypes = tableType.baseTypes();
        boolean[] tableTypeFound = new boolean[baseTableTypes.length];
        // initially flags set to false
        Arrays.fill(tableTypeFound, false);
        // List of files to actually import
        List foundFiles = new ArrayList();

        // if there is only one file, we have the file we want for this type
        // table types must be sorted in order for binary search to work.
        Arrays.sort(baseTableTypes);
        // Not all File objects in the files array need to be read in.
        // Make sure there is one and only one file for each necessary type.
        // Throw exception if there are multiple files for a necessary type.
        // Ignore (do not import) unnecessary files.
        for (int i = 0; i < files.length; i++) {
            String referenceTableType = ReferenceTable.getTableType(files[i].getName());
            // if it is a valid file in the first place (binary search
            // doesn't work for null)
            if (referenceTableType != null) {
                int searchIndex = Arrays.binarySearch(baseTableTypes, referenceTableType);
                // Valid table type. Check for duplicate file names for
                // table type
                // and make sure file name ends with ".txt".
                if (searchIndex >= 0 && files[i].getName().toLowerCase().endsWith(".txt")) {
                    // if no file yet for this table type
                    if (!tableTypeFound[searchIndex]) {
                        // flag that we found a file
                        tableTypeFound[searchIndex] = true;
                        // add to list of files to actually import
                        foundFiles.add(files[i]);
                    }
                    // else
                    else {
                        // already have a file for this table type
                        throw new Exception("Multiple files for table type \"" + referenceTableType
                                + "\" are not allowed in the same directory");
                    }
                }
            }
        }

        // check that a file was found for all table types
        boolean[] tableTypeChecker = new boolean[tableTypeFound.length];
        Arrays.fill(tableTypeChecker, true);
        if (!Arrays.equals(tableTypeFound, tableTypeChecker)) {
            // missing a file for or more table types
            throw new Exception("Missing a file for one or more table types for dataset type \"" + type + "\"");
        }

        return (File[]) foundFiles.toArray(new File[0]);
    }

    /**
     * import a single file into the specified database
     */
    private void importFile(File file, Datasource datasource, FileColumnsMetadata details, boolean overwrite)
            throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String[] columnTypes = details.getColumnTypes();
        String fileName = file.getName();
        String tableType = ReferenceTable.getTableType(fileName);
        if (tableType == null) {
            throw new Exception("Could not determine table type for file name: " + fileName);
        }

        // use the table type to get the table name
        Table table = dataset.getTable(tableType);
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
            throw new Exception("The table \"" + tableName
                    + "\" already exists. Please select 'overwrite tables if exist' or choose a new table name.");
        }

        tableDefinition.createTable(tableName, details.getColumnNames(), columnTypes, null);
        String line = null;
        String[] data = null;
        int numRows = 0;

        // kick out invalid data lines
        int kickOutRows = 0;
        PrintWriter writer = null;
        String canonicalFileName = file.getCanonicalPath();
        int txtIndex = canonicalFileName.indexOf(".txt");
        String writerFileName = "";
        File writerFile = null;
        // find unique file name
        for (int i = 0; writerFile == null || writerFile.exists(); i++) {
            writerFileName = canonicalFileName.substring(0, txtIndex) + ".reimport." + i
                    + canonicalFileName.substring(txtIndex);
            writerFile = new File(writerFileName);
        }

        // read lines in one at a time and put the data into database.. this
        // will avoid huge memory consumption
        while ((line = reader.readLine()) != null) {
            // skip over non data lines as needed
            if (!line.startsWith("#") && line.trim().length() > 0) {
                data = breakUpLine(line, details.getColumnWidths());
                datasource.getDataModifier().insertRow(tableName, data, columnTypes);
                numRows++;
            }
        }// while file is not empty

        // perform capable table type specific processing
        postProcess(datasource, tableName, tableType);

        // when all the data is done ingesting..
        // close the database connections by calling acceptor.finish..
        // and close the reader & writer as well..
        reader.close();
        if (writer != null)
            writer.close();

        if (kickOutRows > 0)
            System.out.println("Kicked out " + kickOutRows + " rows to file " + writerFileName);
    }

    private FileColumnsMetadata getDetails(File file) throws Exception {
        String fileName = file.getName();
        String fileImportType = fileName.substring(0, fileName.length() - 4);
        return fieldDefsReader.getFileColumnsMetadata(fileImportType);
    }

    public void run() throws Exception {
        File file = new File((referenceFilesDir.getPath() + File.separatorChar + REF_DIR_NAME));

        FilenameFilter textFileFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.indexOf(".txt") > 0) {
                    return true;
                }
                return false;
            }
        };
        File[] files = file.listFiles(textFileFilter);

        Dataset dataset = new SimpleDataset();
        ReferenceTableTypes refTableTypes = new ReferenceTableTypes();
        dataset.setDatasetType(refTableTypes.reference());

        dataset.addTable(ReferenceTable.REF_CONTROL_DEVICE_CODES);
        dataset.addTable(ReferenceTable.REF_CONVERSION_FACTORS);
        dataset.addTable(ReferenceTable.REF_EMISSION_TYPES);
        dataset.addTable(ReferenceTable.REF_EMISSION_UNITS_CODES);
        dataset.addTable(ReferenceTable.REF_FIPS);
        dataset.addTable(ReferenceTable.REF_MACT_CODES);
        dataset.addTable(ReferenceTable.REF_MATERIAL_CODES);
        dataset.addTable(ReferenceTable.REF_NAICS_CODES);
        dataset.addTable(ReferenceTable.REF_POLLUTANT_CODES);
        dataset.addTable(ReferenceTable.REF_SCC);
        dataset.addTable(ReferenceTable.REF_SIC_CODES);
        dataset.addTable(ReferenceTable.REF_TIME_ZONES);
        dataset.addTable(ReferenceTable.REF_TRIBAL_CODES);

        run(files, dataset, true);
    }

}
