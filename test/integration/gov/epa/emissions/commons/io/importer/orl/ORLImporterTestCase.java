package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.CommonsTestCase;
import gov.epa.emissions.commons.io.importer.DefaultORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLTableTypes;
import gov.epa.emissions.commons.io.importer.TableType;

public abstract class ORLImporterTestCase extends CommonsTestCase {

    private ORLDatasetTypesFactory types;

    protected void setUp() throws Exception {
        super.setUp();

        this.types = new DefaultORLDatasetTypesFactory();
    }

    abstract protected void doImport(String filename, Dataset dataset) throws Exception;

    public void testNonPoint() throws Exception {
        run("arinv.nonpoint.nti99_NC.txt", types.nonPoint(), ORLTableTypes.ORL_AREA_NONPOINT_TOXICS);
    }

    private void run(String filename, DatasetType datasetType, TableType tableType) throws Exception {
        String table = filename.substring(0, filename.length() - 4).replace('.', '_');

        Dataset dataset = new SimpleDataset();
        dataset.setDatasetType(datasetType.getName());
        // only one base type
        dataset.addTable(new Table(table, tableType.baseTypes()[0]));
        dataset.addTable(new Table(table + "_summary", tableType.summaryType()));

        doImport(filename, dataset);
    }

    public void testNonRoad() throws Exception {
        run("arinv.nonroad.nti99d_NC.new.txt", types.nonRoad(), ORLTableTypes.ORL_AREA_NONROAD_TOXICS);
    }

    public void testNonRoadWithFewerColumns() throws Exception {
        run("Nonroad_withFewerColumns.txt", types.nonRoad(), ORLTableTypes.ORL_AREA_NONROAD_TOXICS);
    }

    public void testPoint() throws Exception {
        run("ptinv.nti99_NC.txt", types.point(), ORLTableTypes.ORL_POINT_TOXICS);
    }

    public void testPointWithApostrophe() throws Exception {
        run("point_withApostrophe.orl", types.point(), ORLTableTypes.ORL_POINT_TOXICS);
    }

    public void testOnRoadMobile() throws Exception {
        run("nti99.NC.onroad.SMOKE.txt", types.onRoad(), ORLTableTypes.ORL_ONROAD_MOBILE_TOXICS);
    }

}
