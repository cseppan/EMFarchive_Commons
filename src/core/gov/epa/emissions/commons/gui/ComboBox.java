package gov.epa.emissions.commons.gui;

import java.awt.Component;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.DefaultComboBoxModel;
import javax.swing.Icon;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.plaf.basic.BasicComboBoxRenderer;

public class ComboBox extends JComboBox implements Changeable {
    private ChangeablesList listOfChangeables;
    
    private boolean changed = false;

    public ComboBox() {
        super();
    }

    public ComboBox(String defaultString, Object[] objects) {
        List list = new ArrayList(Arrays.asList(objects));
        list.add(0, defaultString);
        DefaultComboBoxModel model = new DefaultComboBoxModel(list.toArray());
        setModel(model);
        setRenderer(new ComboBoxRenderer(defaultString));
    }

    public Object getSelectedItem() {
        int index = super.getSelectedIndex();
        return (index > 0) ? super.getSelectedItem() : null;
    }

    class ComboBoxRenderer extends BasicComboBoxRenderer {

        private String defaultString;

        ComboBoxRenderer(String defaultString) {
            this.defaultString = defaultString;
        }

        public Component getListCellRendererComponent(JList list, Object value, int index, boolean isSelected,
                boolean cellHasFocus) {
            if (isSelected) {
                setBackground(list.getSelectionBackground());
                setForeground(list.getSelectionForeground());
            } else {
                setBackground(list.getBackground());
                setForeground(list.getForeground());
            }

            setFont(list.getFont());

            if (value instanceof Icon) {
                setIcon((Icon) value);
            }

            if (value == null) {
                setText(defaultString);
            } else {
                setText(value.toString());
            }
            return ComboBoxRenderer.this;
        }
    }
    
    public void addItemChangeListener() {
        addItemListener(new ItemListener(){
            public void itemStateChanged(ItemEvent e) {
                notifyChanges();
            }
        });
    }
    
    public void clear() {
        this.changed = false;
    }
    
    void notifyChanges() {
        changed = true;
        this.listOfChangeables.onChanges();
    }

    public boolean hasChanges() {
        return this.changed;
    }

    public void observe(ChangeablesList list) {
        this.listOfChangeables = list;
    }
}
