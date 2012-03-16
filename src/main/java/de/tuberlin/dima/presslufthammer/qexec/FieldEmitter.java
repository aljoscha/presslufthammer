package de.tuberlin.dima.presslufthammer.qexec;

import java.io.IOException;

import de.tuberlin.dima.presslufthammer.data.columnar.ColumnReader;
import de.tuberlin.dima.presslufthammer.data.columnar.ColumnWriter;

/**
 * Base class for emitters that write the value of a specific field.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class FieldEmitter {
    private ColumnWriter targetColumn;

    public FieldEmitter(ColumnWriter targetColumn) {
        this.targetColumn = targetColumn;
    }

    public void emit(ColumnReader reader) throws IOException {
        reader.writeToColumn(targetColumn);
    }
}
