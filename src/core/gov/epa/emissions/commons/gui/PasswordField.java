package gov.epa.emissions.commons.gui;

import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import javax.swing.JPasswordField;

public class PasswordField extends JPasswordField implements Changeable {
    private ChangeablesList listOfChangeables;
    
    private boolean changed = false;
    
    public PasswordField(String name, int size) {
        super(size);
        super.setName(name);
    }
    
    public void addListeners() {
        addKeyListener(new KeyAdapter() {
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
