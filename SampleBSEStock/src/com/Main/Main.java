package com.Main;


import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {
	HTable hTable;

	public Main() throws IOException {
		Configuration conf = new Configuration();
		//conf.set("hbase.zookeeper.quorum", "localhost");
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		hTable = new HTable(conf, "stockDataComposite");
	}

	public void putRow(byte[] row, byte[] column, byte[] qaul, byte[] value)
			throws IOException {
		Put put = new Put(row);
		put.add(column, qaul, value);
		hTable.put(put);
	}
	
	public void close() throws IOException{
		hTable.close();
	}

	public static void main(String... arg) throws IOException {
		Main tester = new Main();
		//File folder = new File(arg[0]);
		//File[] fileList = folder.listFiles();
		String columns = "price:open,price:high,price:low,price:close,stats:wap,stats:numShares,stats:numTrades,stats:turnOver,spread:highLow,spread:closeOpen";
		String[] columnList = columns.split(",");
		//for (File file : fileList) {
			//System.out.println("Filename: " + file.getName());
			//FileInputStream fstream = new FileInputStream(
			//		file.getAbsolutePath());
			//DataInputStream in = new DataInputStream(fstream);
			//BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String[] strLine={"50001020110106,712.00,714.00,700.70,706.60,707.116642857650512071,140703,2817,99493433.00,13.30,-5.40"
					,"50001020110107,705.00,709.00,680.10,683.95,691.677274984793441927,179199,4120,123947876.00,28.90,-21.05"
					,"50001020110110,683.95,683.95,651.65,653.60,659.580627600419955671,360038,9292,237474090.00,32.30,-30.35"
					,"50001020110111,662.80,679.00,650.00,660.70,659.903482362922757938,608179,6143,401339440.00,29.00,-2.10"
					,"50001020110112,669.80,687.00,650.25,681.35,666.587045732295619733,310940,7188,207268576.00,36.75,11.55"
					};
	for(int i=0;i<strLine.length;i++){
			//while ((strLine = br.readLine()) != null) {
				String[] columnsValue = strLine[i].split(",");
				if (columnsValue.length == 11) {
					String stockId = columnsValue[0].substring(0, 6);
					System.out.println(stockId);
					
					String date = columnsValue[0].substring(6);
					System.out.println(date);
					byte[] row = Bytes.add(
							Bytes.toBytes(stockId),
							Bytes.toBytes(Long.parseLong(date)));
					
					for (int k = 0; k < 10; k++) {
						String[] columnQual = columnList[k].split(":");
						byte[] col = Bytes.toBytes(columnQual[0]);
						byte[] qaul = Bytes.toBytes(columnQual[1]);
						byte[] value = null;
						if(columnQual[1].equalsIgnoreCase("numShares") || columnQual[1].equalsIgnoreCase("numTrades")){
							value = Bytes.toBytes(Long.parseLong(columnsValue[k + 1]));
						} else {
						value = Bytes.toBytes(Float.parseFloat(columnsValue[k + 1]));
						}
						
						tester.putRow(row, col, qaul, value);
					}
				}
	}
			//}
			//br.close();
			//in.close();
			//fstream.close();
		//}
		tester.close();
	}
}