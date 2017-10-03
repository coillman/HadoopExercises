import java.util.ArrayList;
import java.util.Comparator;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class PivotMapReduce {

	public void map(int key, ArrayList<String> values, Context context){
		for(int i=0; i<values.size();i++){
			context.write(i, key + values.get(i));
		}
	}
	
	public void reduce(int key, ArrayList<String> values, Context context){
		ArrayList<String> reducedVal = new ArrayList<String>();
		
		values.sort(new Comparator<String>(){
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
			
		});
		
		for(int i=0; i<values.size();i++){
			reducedVal.add(values.get(i).substring(1));
		}
		context.write(key, reducedVal);
	}
}
