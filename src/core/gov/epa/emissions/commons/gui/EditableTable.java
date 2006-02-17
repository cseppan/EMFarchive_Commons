package gov.epa.emissions.commons.gui;

import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;

import javax.swing.JTable;
import javax.swing.table.TableModel;

public class EditableTable extends JTable implements Editor,Changeable {
    private ChangeablesList listOfChangeables;
    
    private boolean changed = false;
    
    public EditableTable(TableModel tableModel) {
        super(tableModel);
        setRowHeight(25);
    }

    public void commit() {
        if (isEditing()) {
            getCellEditor().stopCellEditing();
        }
    }
    
    public void addListeners() {
        addKeyListener(new KeyListener() {
            public void keyPressed(KeyEvent e) {
                notifyChanges();
            }
            
            public void keyReleased(KeyEvent e) {
                notifyChanges();
            }
            
            public void keyTyped(KeyEvent e) {
                notifyChanges();
            }
        });
    }
    
    public void clear() {
        this.changed = false;
    }
    
    private void notifyChanges() {
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
