package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.DefaultORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLTableType;
import gov.epa.emissions.commons.io.importer.ORLTableTypes;
import gov.epa.emissions.commons.io.importer.orl.BaseORLImporter;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ORLExporterTest extends DbTestCase {

    private ORLDatasetTypesFactory types;

    private ORLTableTypes orlTableTypes;

    protected void setUp() throws Exception {
        super.setUp();

        this.types = new DefaultORLDatasetTypesFactory();
        orlTableTypes = new ORLTableTypes(types);
    }

    public void testPoint() throws Exception {
        doImport("ptinv.nti99_NC.txt", types.point(), orlTableTypes.point());

        DatasetType datasetType = types.point();
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, orlTableTypes.point(), tableName, file);
    }

    public void testExportSucceedsUsingDefaultSettingsEvenIfFileExists() throws Exception {
        doImport("ptinv.nti99_NC.txt", types.point(), orlTableTypes.point());

        DatasetType datasetType = types.point();
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);
        file.delete();
        assertTrue(file.createNewFile());

        doExport(datasetType, orlTableTypes.point(), tableName, file);
    }

    public void testExportSucceedsWhenOverwriteIsDisabledAndOutputFileDoesNotExist() throws Exception {
        DatasetType datasetType = types.point();
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);

        doImport("ptinv.nti99_NC.txt", datasetType, orlTableTypes.point());

        file.delete();
        doExportWithoutOverwrite(datasetType, orlTableTypes.point(), tableName, file);
    }

    public void testExportFailsWhenOverwriteIsDisabledAndOutputFileExists() throws Exception {
        DatasetType datasetType = types.point();
        String tableName = "ptinv_nti99_NC";
        File file = createFile(datasetType, tableName);

        doImport("ptinv.nti99_NC.txt", datasetType, orlTableTypes.point());
        doExport(datasetType, orlTableTypes.point(), tableName, file);

        try {
            doExportWithoutOverwrite(datasetType, orlTableTypes.point(), tableName, file);
        } catch (Exception e) {
            return;
        }

        fail("should have failed to export since file has already been exported and exists");
    }

    public void testNonPoint() throws Exception {
        doImport("arinv.nonpoint.nti99_NC.txt", types.nonPoint(), orlTableTypes.nonPoint());

        DatasetType datasetType = types.nonPoint();
        String tableName = "arinv_nonpoint_nti99_NC";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, orlTableTypes.nonPoint(), tableName, file);
    }

    public void testOnRoadMobile() throws Exception {
        doImport("nti99.NC.onroad.SMOKE.txt", types.onRoad(), orlTableTypes.onRoad());

        DatasetType datasetType = types.onRoad();
        String tableName = "nti99_NC_onroad_SMOKE";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, orlTableTypes.onRoad(), tableName, file);
    }

    public void testNonRoad() throws Exception {
        doImport("arinv.nonroad.nti99d_NC.new.txt", types.nonRoad(), orlTableTypes.nonRoad());

        DatasetType datasetType = types.nonRoad();
        String tableName = "arinv_nonroad_nti99d_NC_new";
        File file = createFile(datasetType, tableName);

        doExport(datasetType, orlTableTypes.nonRoad(), tableName, file);
    }

    private void doExportWithoutOverwrite(DatasetType datasetType, ORLTableType tableType, String tableName, File file)
            throws Exception {
        ORLExporter exporter = ORLExporter.createWithoutOverwrite(dbSetup.getDbServer(), types);
        Dataset dataset = createDataset(datasetType, tableType, tableName);

        exporter.run(dataset, file);
    }

    private void doExport(DatasetType datasetType, ORLTableType tableType, String tableName, File file)
            throws Exception {
        ORLExporter exporter = ORLExporter.create(dbSetup.getDbServer(), types);
        Dataset dataset = createDataset(datasetType, tableType, tableName);

        exporter.run(dataset, file);
    }

    private File createFile(DatasetType datasetType, String tableName) {
        String tempDir = System.getProperty("java.io.tmpdir");
        String exportFileName = tempDir + "/" + trimSpaces(datasetType) + "." + tableName + ".EXPORTED_";

        File file = new File(exportFileName);
        file.deleteOnExit();

        return file;
    }

    private String trimSpaces(DatasetType datasetType) {
        Matcher m = Pattern.compile("\\s").matcher(datasetType.getName());
        return m.replaceAll("");
    }

    private Dataset createDataset(DatasetType datasetType, ORLTableType tableType, String tableName) {
        Dataset dataset = new SimpleDataset();
        dataset.setDatasetType(datasetType);
        // only one base type
        dataset.addTable(new Table(tableName, tableType.base()));
        dataset.setRegion("US");
        dataset.setCountry("US");
        dataset.setYear(1234);
        dataset.setDescription("This is the first line of an artificial description\nThis is the second line");

        return dataset;
    }

    private void doImport(final String filename, DatasetType type, ORLTableType tableType) throws Exception {
        String tableName = filename.substring(0, filename.length() - 4).replace('.', '_');

        Dataset dataset = new SimpleDataset();
        dataset.setDatasetType(type);
        dataset.addTable(new Table(tableName, tableType.base()));

        Importer importer = new BaseORLImporter(dbSetup.getDbServer(), true, types);
        importer.run(new File[] { new File("test/data/orl/nc", filename) }, dataset, true);
    }
}
