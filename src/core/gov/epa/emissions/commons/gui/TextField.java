package gov.epa.emissions.commons.gui;

import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.Document;

public class TextField extends JTextField implements Changeable {
    private ChangeablesList listOfChangeables;

    private boolean changed = false;

    public TextField(String name, int size) {
        super(size);
        super.setName(name);
    }

    public TextField(String name, String value, int size) {
        this(name, size);
        super.setText(value);
    }

    public void addTextListener() {
        Document nameDoc = this.getDocument();
        nameDoc.addDocumentListener(new DocumentListener() {
            public void changedUpdate(DocumentEvent e) {
                notifyChanges();
            }

            public void insertUpdate(DocumentEvent e) {
                notifyChanges();
            }

            public void removeUpdate(DocumentEvent e) {
                notifyChanges();
            }
        });
    }
    
    public void addKeyListener() {
        this.addKeyListener(new KeyAdapter() {
            public void keyTyped(KeyEvent e) {
                notifyChanges();
            }
        });
    }

    public void clear() {
        this.changed = false;
    }

    private void notifyChanges() {
        changed = true;
        this.listOfChangeables.onChanges();
    }

    public boolean hasChanges() {
        return this.changed;
    }

    public void observe(ChangeablesList list) {
        this.listOfChangeables = list;
    }

    public boolean isEmpty() {
        return getText().trim().length() == 0;
    }
}
