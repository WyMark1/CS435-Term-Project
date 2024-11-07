package Accidents;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MapperWriteable implements Writable {
    private double[] values;
    private int[] count;  

    public MapperWriteable(double[] values, int[] count) {
        this.values = values;
        this.count = count;
    }

    public MapperWriteable() {
    }

    public void set(double[] values, int[] count) {
        this.values = values;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length); 
        for (double d : values) {
            out.writeDouble(d);
        }

        out.writeInt(count.length); 
        for (int c : count) {
            out.writeInt(c);  
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int valuesLength = in.readInt();
        values = new double[valuesLength];
        for (int i = 0; i < valuesLength; i++) {
            values[i] = in.readDouble();
        }

        int countLength = in.readInt();  
        count = new int[countLength];
        for (int i = 0; i < countLength; i++) {
            count[i] = in.readInt();  
        }
    }

    public double[] getValues() {
        return values;
    }

    public int[] getCount() {  
        return count;
    }
}

