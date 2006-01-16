package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.TemporalResolution;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class IDAImporter {

    private Datasource emissionDatasource;

    private Datasource referenceDatasource;

    private Dataset dataset;

    private SqlDataTypes sqlDataTypes;

    private DatasetTypeUnit unit;

    private FileVerifier fileVerifier;

    private File file;

    private DataTable dataTable;

    public IDAImporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        this.dataset = dataset;
        this.emissionDatasource = dbServer.getEmissionsDatasource();
        this.referenceDatasource = dbServer.getReferenceDatasource();
        this.sqlDataTypes = sqlDataTypes;
        this.fileVerifier = new FileVerifier();
    }

    public void setup(File folder, String[] fileNames, IDAFileFormat fileFormat) throws ImporterException {
        File file = new File(folder,fileNames[0]);
        fileVerifier.shouldExist(file);
        this.file = file;
        IDAHeaderReader headerReader = new IDAHeaderReader(file);
        headerReader.read();
        headerReader.close();

        fileFormat.addPollutantCols(headerReader.polluntants());
        IDATableFormat tableFormat = new IDATableFormat(fileFormat, sqlDataTypes);

        unit = new DatasetTypeUnit(tableFormat, fileFormat);
        DatasetLoader loader = new DatasetLoader(dataset);
        
        dataTable = new DataTable(dataset, emissionDatasource);
        InternalSource internalSource = loader.internalSource(file, dataTable.name(), tableFormat);
        unit.setInternalSource(internalSource);

        validateIDAFile(headerReader.comments());
    }

    public void run() throws ImporterException {
        dataTable.create(unit.tableFormat());
        try {
            doImport(unit, dataset, dataTable.name());
        } catch (Exception e) {
            dataTable.drop();
            throw new ImporterException("Filename: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    private void doImport(DatasetTypeUnit unit, Dataset dataset, String table) throws Exception {
        Reader idaReader = new IDAFileReader(unit.getInternalSource().getSource(), unit.fileFormat());
        IDADataLoader loader = new IDADataLoader(emissionDatasource, referenceDatasource, unit.tableFormat());
        loader.load(idaReader, dataset, table);
        loadDataset(file, table, unit.tableFormat(), idaReader.comments(), dataset);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, List commentsList, Dataset dataset) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
        dataset.setUnits("short tons/year");
        Comments comments = new Comments(commentsList);
        dataset.setDescription(comments.all());
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
    }

    private void validateIDAFile(List comments) throws ImporterException {
        addAttributesExtractedFromComments(comments, dataset);
    }

    private void addAttributesExtractedFromComments(List commentsList, Dataset dataset) throws ImporterException {
        Comments comments = new Comments(commentsList);
        if (!comments.have("IDA"))
            throw new ImporterException("The tag - 'IDA' is mandatory.");

        if (!comments.have("COUNTRY"))
            throw new ImporterException("The tag - 'COUNTRY' is mandatory.");
        // TODO:Support all countries, Currently only files from US is supported
        String country = comments.content("COUNTRY");
        if (!country.toLowerCase().equals("us"))
            throw new ImporterException("Currently the IDA importer supports files for US not for '" + country + "'");
        dataset.setCountry(country);
        dataset.setRegion(country);

        if (!comments.have("YEAR"))
            throw new ImporterException("The tag - 'YEAR' is mandatory.");
        String year = comments.content("YEAR");
        dataset.setYear(Integer.parseInt(year));
        setStartStopDateTimes(dataset, Integer.parseInt(year));
    }

    private void setStartStopDateTimes(Dataset dataset, int year) {
        Date start = new GregorianCalendar(year, Calendar.JANUARY, 1).getTime();
        dataset.setStartDateTime(start);

        Calendar endCal = new GregorianCalendar(year, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        dataset.setStopDateTime(endCal.getTime());
    }

}
