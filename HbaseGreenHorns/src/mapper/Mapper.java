package mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * The map method splits the csv file according to this structure
     * brand,model,size (e.g. Cadillac,Seville,Midsize) and output all data using
     * brand as key and the couple model,size as value.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] valueSplitted = value.toString().split(",");
        if (valueSplitted.length == 3) {
            String brand = valueSplitted[0];
            String model = valueSplitted[1];
            String size = valueSplitted[2];

            outKey.set(brand);
            outValue.set(model + "," + size);
            System.out.println("key===>>"+outKey+"Value===>>"+outValue);
            context.write(outKey, outValue);
        }
    }
}
