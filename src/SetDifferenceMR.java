import java.util.*;

public class SetDifferenceMR extends MyMapReduce {

	public SetDifferenceMR(KVPair[] data, int num_map_tasks, int num_reduce_tasks) {
		super(data, num_map_tasks, num_reduce_tasks);
	}

	@Override
	public ArrayList<KVPair> map(KVPair kv) {
		// TODO Auto-generated method stub
		ArrayList<KVPair> set = new ArrayList<KVPair>();
		
		if(kv.v instanceof int[]){
			for(int record: (int[])kv.v){
				KVPair kvNew = new KVPair(record, kv.k);
				set.add(kvNew);
			}
		}else if(kv.v instanceof String[]){
			for(String record: (String[])kv.v){
				KVPair kvNew = new KVPair(record, kv.k);
				set.add(kvNew);
			}
		}
		
		return set;
	}

	@Override
	public KVPair reduce(KVPair kv) {
		// TODO Auto-generated method stub
		
		List<Character> list_C = (ArrayList<Character>)kv.v;
		
		//__VAISHALI_09_24_2017__Return null values if the values has set S in it
		if(!list_C.contains('S'))
			return new KVPair(kv.k,list_C.get(0).toString());
		else return new KVPair(null,null);
		
	}
	
}
