package gov.epa.emissions.commons.gui;

import javax.swing.JTable;

public class EditableTable extends JTable implements Editor, Changeable {
    private ChangeablesList listOfChangeables;

    private boolean changed = false;

    private EditableTableModel tableModel;

    public EditableTable(EditableTableModel tableModel) {
        super(tableModel);
        this.tableModel = tableModel;
        setRowHeight(25);
    }

    public void setValueAt(Object value, int row, int column) {
        super.setValueAt(value, row, column);
        if (tableModel.shouldTrackChange(column))
            notifyChanges();
    }

    public void commit() {
        if (isEditing()) {
            getCellEditor().stopCellEditing();
        }
    }

    public void clear() {
        this.changed = false;
    }

    void notifyChanges() {
        this.changed = true;
        this.listOfChangeables.onChanges();
    }

    public boolean hasChanges() {
        return this.changed;
    }

    public void observe(ChangeablesList list) {
        this.listOfChangeables = list;
    }

}
