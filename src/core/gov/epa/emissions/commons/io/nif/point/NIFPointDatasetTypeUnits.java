package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FixedColsTableFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImportHelper;
import gov.epa.emissions.commons.io.nif.NIFDatasetTypeUnits;

import java.io.File;

public class NIFPointDatasetTypeUnits implements NIFDatasetTypeUnits {

    private FormatUnit ceDatasetTypeUnit;

    private FormatUnit emDatasetTypeUnit;

    private FormatUnit epDatasetTypeUnit;

    private FormatUnit peDatasetTypeUnit;

    private DatasetTypeUnit erDatasetTypeUnit;

    private DatasetTypeUnit euDatasetTypeUnit;

    private DatasetTypeUnit siDatasetTypeUnit;

    private NIFImportHelper delegate;

    private File[] files;

    private String tablePrefix;

    public NIFPointDatasetTypeUnits(File[] files, String tablePrefix, SqlDataTypes sqlDataTypes) {
        this.files = files;
        this.tablePrefix = tablePrefix;
        FileFormat ceFileFormat = new ControlEquipmentFileFormat(sqlDataTypes);
        ceDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(ceFileFormat, sqlDataTypes), ceFileFormat,
                false);

        FileFormat emFileFormat = new EmissionRecordsFileFormat(sqlDataTypes);
        emDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(emFileFormat, sqlDataTypes), emFileFormat,
                false);

        FileFormat epFileFormat = new EmissionProcessFileFormat(sqlDataTypes);
        epDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(epFileFormat, sqlDataTypes), epFileFormat,
                false);

        FileFormat erFileFormat = new EmissionReleasesFileFormat(sqlDataTypes);
        erDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(erFileFormat, sqlDataTypes), erFileFormat,
                false);

        FileFormat euFileFormat = new EmissionUnitsFileFormat(sqlDataTypes);
        euDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(euFileFormat, sqlDataTypes), euFileFormat,
                false);

        FileFormat peFileFormat = new EmissionPeriodsFileFormat(sqlDataTypes);
        peDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(peFileFormat, sqlDataTypes), peFileFormat,
                false);

        FileFormat siFileFormat = new EmissionSitesFileFormat(sqlDataTypes);
        siDatasetTypeUnit = new DatasetTypeUnit(new FixedColsTableFormat(siFileFormat, sqlDataTypes), siFileFormat,
                false);
        delegate = new NIFImportHelper();
    }

    public void process() throws ImporterException {
        associateFileWithUnit(files, tablePrefix);
        requiredExist();
    }

    public FormatUnit[] formatUnits() {
        return new FormatUnit[] { ceDatasetTypeUnit, emDatasetTypeUnit, epDatasetTypeUnit, erDatasetTypeUnit,
                euDatasetTypeUnit, peDatasetTypeUnit, siDatasetTypeUnit };
    }
    
    public String dataTable(){
        return emDatasetTypeUnit.getInternalSource().getTable();
    }

    private void requiredExist() throws ImporterException {
        FormatUnit[] reqUnits = { emDatasetTypeUnit, epDatasetTypeUnit, erDatasetTypeUnit, euDatasetTypeUnit,
                peDatasetTypeUnit, siDatasetTypeUnit };
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < reqUnits.length; i++) {
            if (reqUnits[i].getInternalSource() == null) {
                sb.append("\t" + reqUnits[i].fileFormat().identify() + "\n");
            }
        }

        if (sb.length() > 0) {
            throw new ImporterException("NIF point import requires following types \n" + sb.toString());
        }
    }

    private void associateFileWithUnit(File[] files, String tableName) throws ImporterException {
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            String key = delegate.notation(file);
            FormatUnit formatUnit = fileToDatasetTypeUnit(key);
            if (formatUnit != null) {
                formatUnit.setInternalSource(delegate.internalSource(tableName, key, file, formatUnit));
            }
        }
    }

    private FormatUnit fileToDatasetTypeUnit(String key) {
        if ("ce".equals(key)) {
            return ceDatasetTypeUnit;
        }

        if ("em".equals(key)) {
            return emDatasetTypeUnit;
        }

        if ("ep".equals(key)) {
            return epDatasetTypeUnit;
        }

        if ("eu".equals(key)) {
            return euDatasetTypeUnit;
        }

        if ("er".equals(key)) {
            return erDatasetTypeUnit;
        }

        if ("pe".equals(key)) {
            return peDatasetTypeUnit;
        }

        if ("si".equals(key)) {
            return siDatasetTypeUnit;
        }
        return null;
    }

}
