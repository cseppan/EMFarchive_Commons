package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.VersionedTableFormatWithOptionalCols;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class VersionedRecordsWriter {

    private PreparedStatement updateStatement;

    private String table;

    private VersionedTableFormatWithOptionalCols tableFormat;

    private Datasource datasource;

    public VersionedRecordsWriter(Datasource datasource, String table, SqlDataTypes types) throws SQLException {
        this.datasource = datasource;
        this.table = table;

        FileFormatWithOptionalCols fileFormat = fileFormat(datasource.dataModifier(), table);
        this.tableFormat = new VersionedTableFormatWithOptionalCols(fileFormat, types);

        String dataUpdate = "UPDATE " + datasource.getName() + "." + table + " SET delete_versions=? WHERE record_id=?";
        Connection connection = datasource.getConnection();
        updateStatement = connection.prepareStatement(dataUpdate);
    }

    private FileFormatWithOptionalCols fileFormat(DataModifier modifier, String table) throws SQLException {
        final Column[] cols = modifier.getColumns(table);
        return new FileFormatWithOptionalCols() {
            public Column[] optionalCols() {
                return null;
            }

            public Column[] minCols() {
                return cols;
            }

            public String identify() {
                return null;
            }

            public Column[] cols() {
                return minCols();
            }
        };
    }

    /**
     * ChangeSet contains adds, deletes, and updates. An update is treated as a
     * combination of 'delete' and 'add'. In effect, the ChangeSet is written as
     * a list of 'delete' and 'add' operations.
     */
    public void update(ChangeSet changeset) throws Exception {
        convertUpdatedRecords(changeset);
        writeData(changeset);
    }

    public void close() throws SQLException {
        updateStatement.close();
    }

    private void convertUpdatedRecords(ChangeSet changeset) {
        VersionedRecord[] updatedRecords = changeset.getUpdated();

        for (int i = 0; i < updatedRecords.length; i++) {
            VersionedRecord deleteRec = updatedRecords[i];
            deleteRec.setDeleteVersions(deleteRec.getDeleteVersions() + "," + changeset.getVersion().getVersion());
            changeset.addDeleted(deleteRec);

            VersionedRecord insertRec = updatedRecords[i];
            changeset.addNew(insertRec);
        }
    }

    private void writeData(ChangeSet changeset) throws Exception {
        insertData(changeset.getNew(), changeset.getVersion());
        deleteData(changeset.getDeleted(), changeset.getVersion());
    }

    private void insertData(VersionedRecord[] records, Version version) throws Exception {
        DataModifier modifier = datasource.dataModifier();
        for (int i = 0; i < records.length; i++) {
            List data = tableFormat.fill(records[i], version.getVersion());

            String[] toArray = (String[]) data.toArray(new String[0]);
            modifier.insertRow(table, toArray);
        }
    }

    private void deleteData(VersionedRecord[] records, Version version) throws SQLException {
        for (int i = 0; i < records.length; i++) {
            updateStatement.setString(1, version.getVersion() + "");
            updateStatement.setInt(2, records[i].getRecordId());
            updateStatement.execute();
        }
    }

}
