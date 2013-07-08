package reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Reducer  extends org.apache.hadoop.mapreduce.Reducer<Text, Text, ImmutableBytesWritable, Writable> {
	 
    /**
     * The reduce method fill the TestCars table with all csv data,
     * compute some counters and save those counters into the TestBrandsSizes table.
     * So we use two different HBase table as output for the reduce method.
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> statsSizeCounters = new HashMap<String, Integer>();
        String brand = key.toString();
        // We are receiving all models,size grouped by brand.
        for (Text value : values) {
            String[] valueSplitted = value.toString().split(",");
            if (valueSplitted.length == 2) {
                String model = valueSplitted[0];
                String size = valueSplitted[1];

                // Fill the TestCars table
                ImmutableBytesWritable putTable = new ImmutableBytesWritable(Bytes.toBytes("TestCars"));
                byte[] putKey = Bytes.toBytes(brand + "," + model);
                byte[] putFamily = Bytes.toBytes("Car");
                Put put = new Put(putKey);
                // qualifier brand
                byte[] putQualifier = Bytes.toBytes("brand");
                byte[] putValue = Bytes.toBytes(brand);
                put.add(putFamily, putQualifier, putValue);
                // qualifier model
                putQualifier = Bytes.toBytes("model");
                putValue = Bytes.toBytes(model);
                put.add(putFamily, putQualifier, putValue);
                // qualifier size
                putQualifier = Bytes.toBytes("size");
                putValue = Bytes.toBytes(size);
                put.add(putFamily, putQualifier, putValue);
                context.write(putTable, put);

                // Compute some counters: number of different sizes for a brand
                if (!statsSizeCounters.containsKey(size))
                    statsSizeCounters.put(size, 1);
                else
                    statsSizeCounters.put(size, statsSizeCounters.get(size) + 1);
            }
        }

        for (Entry<String, Integer> entry : statsSizeCounters.entrySet()) {
            // Fill the TestBrandsSizes table
            ImmutableBytesWritable putTable = new ImmutableBytesWritable(Bytes.toBytes("TestBrandsSizes"));
            byte[] putKey = Bytes.toBytes(brand);
            byte[] putFamily = Bytes.toBytes("BrandSizes");
            Put put = new Put(putKey);
            // We can use as qualifier the sizes
            byte[] putQualifier = Bytes.toBytes(entry.getKey());
            byte[] putValue = Bytes.toBytes(entry.getValue());
            put.add(putFamily, putQualifier, putValue);
            context.write(putTable, put);
        }
    }
}
