package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.DataReader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedWidthParser;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NIFImporter {

    private Dataset dataset;
    
    private NIFDatasetTypeUnits datasetTypeUnits;
    
    private Datasource datasource;

    private List tableNames;

    private HelpImporter delegate;

    public NIFImporter(Dataset dataset, NIFDatasetTypeUnits datasetTypeUnits, Datasource datasource) {
        this.dataset = dataset;
        this.datasetTypeUnits = datasetTypeUnits;
        this.datasource = datasource;
        this.tableNames = new ArrayList();
        this.delegate = new HelpImporter();
    }

    public void preImport() throws ImporterException {
        InternalSource[] internalSources = dataset.getInternalSources();
        for(int i=0; i< internalSources.length; i++){
            delegate.validateFile(new File(internalSources[i].getSource()));
        }
        datasetTypeUnits.processFiles(internalSources);
    }


    public void run() throws ImporterException {
        FormatUnit[] units = datasetTypeUnits.formatUnits();
        for (int i = 0; i < units.length; i++) {
            doImport(dataset, units[i]);
        }
    }

    private void doImport(Dataset dataset, FormatUnit unit) throws ImporterException {
        InternalSource internalSource = unit.getInternalSource();
        if (internalSource == null) {
            return;
        }
        doImport(internalSource, unit, dataset);
    }
    
    private void doImport(InternalSource internalSource, FormatUnit unit, Dataset dataset) throws ImporterException {
        String tableName = internalSource.getTable();
        String source = internalSource.getSource();
        delegate.createTable(tableName,datasource, unit.tableFormat(), dataset.getName());
        tableNames.add(tableName);
        try {
            doImport(source, dataset, tableName, unit.fileFormat(), unit.tableFormat());
        } catch (Exception e) {
            dropTables(tableNames);
            throw new ImporterException("Filename: " + source +", "+ e.getMessage());
        }
    }

    private void doImport(String fileName, Dataset dataset, String tableName, FileFormat fileFormat,
            TableFormat tableFormat) throws ImporterException, IOException {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        Reader fileReader = new DataReader(reader, new FixedWidthParser(fileFormat));
        loader.load(fileReader, dataset, tableName);
        reader.close();
        loadDataset(fileFormat,fileReader.comments(),dataset);
    }


    //TODO: load starttime, endtime
    private void loadDataset(FileFormat fileFormat, List comments, Dataset dataset) {
        updateInternalSources(fileFormat,dataset);
        dataset.setDescription(delegate.descriptions(comments));
        
    }
    
    private void updateInternalSources(FileFormat fileFormat, Dataset dataset) {
        InternalSource[] internalSources = dataset.getInternalSources();
        for(int i=0; i< internalSources.length; i++){
            internalSources[i].setType(fileFormat.identify());
            internalSources[i].setCols(colNames(fileFormat.cols()));
        }
    }
    
    private String[] colNames(Column[] cols) {
        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++)
            names.add(cols[i].name());

        return (String[]) names.toArray(new String[0]);
    }

    private void dropTables(List tableNames) throws ImporterException {
        for (int i = 0; i < tableNames.size(); i++) {
            delegate.dropTable((String) tableNames.get(i),datasource);
        }
    }

}
