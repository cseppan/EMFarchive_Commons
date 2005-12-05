package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TemporalResolution;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

public class IDAImporter {

    private Datasource datasource;

    private Dataset dataset;

    private SqlDataTypes sqlDataTypes;

    private DatasetTypeUnit unit;

    private HelpImporter delegate;

    private File file;

    public IDAImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.dataset = dataset;
        this.datasource = datasource;
        this.sqlDataTypes = sqlDataTypes;
        this.delegate = new HelpImporter();
    }

    public void setup(File file, IDAFileFormat fileFormat) throws ImporterException {
        delegate.validateFile(file);
        this.file = file;
        IDAHeaderReader headerReader = new IDAHeaderReader(file);
        headerReader.read();
        headerReader.close();

        fileFormat.addPollutantCols(headerReader.polluntants());
        FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);

        unit = new DatasetTypeUnit(tableFormat, fileFormat);
        unit.setInternalSource(internalSource(file,dataset.getName()));

        validateIDAFile(headerReader.comments());
    }

    private InternalSource internalSource(File file, String datasetName) {
        InternalSource internalSource = new InternalSource();
        internalSource.setSource(file.getAbsolutePath());
        internalSource.setTable(delegate.tableName(datasetName));
        internalSource.setSourceSize(file.length());
        
        return internalSource;
    }

    public void run() throws ImporterException {
        String table = unit.getInternalSource().getTable();
        delegate.createTable(table, datasource, unit.tableFormat(), dataset.getName());
        try {
            doImport(unit, dataset, table);
        } catch (Exception e) {
            delegate.dropTable(table, datasource);
            throw new ImporterException("Filename: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    private void doImport(DatasetTypeUnit unit, Dataset dataset, String table) throws Exception {

        Reader idaReader = new IDAFileReader(unit.getInternalSource().getSource(), unit.fileFormat());
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, unit.tableFormat());
        loader.load(idaReader, dataset, table);
        loadDataset(file, table, unit.fileFormat(), idaReader.comments(), dataset);
    }

    private void loadDataset(File file, String table, FileFormat fileFormat, List comments, Dataset dataset) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
        dataset.setUnits("short tons/year");
        dataset.setDescription(delegate.descriptions(comments));
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
        dataset.setDescription(delegate.descriptions(comments));
    }

    private void validateIDAFile(List comments) throws ImporterException {
        addAttributesExtractedFromComments(comments, dataset);
    }

    private void addAttributesExtractedFromComments(List comments, Dataset dataset) throws ImporterException {
        if (!(tag("#IDA", comments) != null))
            throw new ImporterException("The tag - 'IDA' is mandatory.");

        String country = tag("#COUNTRY", comments);
        if (country == null || country.length() == 0)
            throw new ImporterException("The tag - 'COUNTRY' is mandatory.");
        // TODO:Support all countries, Currently only files from US is supported
        if(!country.toLowerCase().equals("us"))
            throw new ImporterException("Currently the IDA importer supports files for US not for '"+country+"'");
        dataset.setCountry(country);
        dataset.setRegion(country);

        String year = tag("#YEAR", comments);
        if (year == null || year.length() == 0)
            throw new ImporterException("The tag - 'YEAR' is mandatory.");
        dataset.setYear(Integer.parseInt(year));
        setStartStopDateTimes(dataset, Integer.parseInt(year));
    }

    private String tag(String tag, List comments) {
        for (Iterator iter = comments.iterator(); iter.hasNext();) {
            String comment = (String) iter.next();
            if (comment.trim().startsWith(tag)) {
                return comment.substring(tag.length()).trim();
            }
        }
        return null;
    }

    private void setStartStopDateTimes(Dataset dataset, int year) {
        Date start = new GregorianCalendar(year, Calendar.JANUARY, 1).getTime();
        dataset.setStartDateTime(start);

        Calendar endCal = new GregorianCalendar(year, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        dataset.setStopDateTime(endCal.getTime());
    }

}
