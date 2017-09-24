//########################################
//## Template Code for Big Data Analytics
//## assignment 1, at Stony Brook University
//## Fall 2017

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

//##########################################################################
//##########################################################################
//# PART I. MapReduce

public abstract class MyMapReduce {
	KVPair[] data;
	int num_map_tasks=5;
	int num_reduce_tasks=3;
	final SharedVariables svs = new SharedVariables();
	class SharedVariables{
		public volatile List<ReducerTask> namenode_m2r = Collections.synchronizedList(new ArrayList<ReducerTask>());
		public volatile List<KVPair> namenode_fromR = Collections.synchronizedList(new ArrayList<KVPair>());
	}
	
	public MyMapReduce(KVPair[] data, int num_map_tasks, int num_reduce_tasks){
		this.data=data;
		this.num_map_tasks=num_map_tasks;
		this.num_reduce_tasks=num_reduce_tasks;
	}
	
    //###########################################################   
    //#programmer methods (to be overridden by inheriting class)
	
	public abstract ArrayList<KVPair> map(KVPair kv);
	public abstract KVPair reduce(KVPair kv);
	
    //###########################################################
    //#System Code: What the map reduce backend handles
	public void mapTask(KVPair[] data_chunk, List<ReducerTask> namenode_m2r){
		//#runs the mappers and assigns each k,v to a reduce task
		for (KVPair kv : data_chunk){
			//#run mappers:
			System.out.println(kv.v);
			ArrayList<KVPair> mapped_kvs = this.map(kv);
			//#assign each kv pair to a reducer task
			for (KVPair mapped_kv:mapped_kvs){
				namenode_m2r.add(new ReducerTask(this.partitionFunction(mapped_kv.k.toString()),mapped_kv));
			}
		}
	}
	
	public int partitionFunction(String k){
		//#given a key returns the reduce task to send it
		int node_number=0;
		//#implement this method
		
		//__VAISHALI_09_17_2017__Using inbuilt hashcode() function to get hash on the string
		// and use the modulo to get the reducer number as per the hash
		node_number = Math.abs(k.hashCode()) % this.num_reduce_tasks;
		return node_number;
	}
	
	
	public void reduceTask(ArrayList<KVPair> kvs, List<KVPair> namenode_fromR){
        //#sort all values for each key into a list 
        //#[TODO]
		//__VAISHALI_09_17_2017__First sort as per the key using given compareTo function
		Collections.sort(kvs,(a, b) -> b.compareTo(a));
		
		//__VAISHALI_09_17_2017__Grouping the values as per key using Java Map API
		Map<Object, List<Object>> map = new HashMap<Object, List<Object>>();
		for (KVPair kv : kvs) {
			Object key  = kv.k;
		    if(map.containsKey(key)){
		        map.get(key).add(kv.v);
		    }else{
		        List<Object> list = new ArrayList<Object>();
		        list.add(kv.v);
		        map.put(key, list);
		    }
		}

        //#call reducers on each key paired with a *list* of values
        //#and append the result for each key to namenode_fromR
        //#[TODO]
		//__VAISHALI_09_17_2017__Converting Map to list KVPair for easy call to reduce
		List<KVPair> kv_group = new ArrayList<KVPair>();
		for(Object key: map.keySet()){
			KVPair kv = new KVPair(key,map.get(key));
			kv_group.add(kv);
		}
		
		for(KVPair kv : kv_group){
			namenode_fromR.add(this.reduce(kv));
		}
	}
	
	public List<KVPair> runSystem() throws ExecutionException, InterruptedException{
		/*#runs the full map-reduce system processes on mrObject
		
        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]*/
		
		/*#divide up the data into chunks accord to num_map_tasks, launch a new process
         *#for each map task, passing the chunk of data to it. 
         *#hint: if a chunk contains the data going to a given maptask then the following
         *#      starts a process (Thread or Future class are possible data types for this variable)
         *#      
         *		if you want to use Future class, you can define variables like this:
         *		manager = Executors.newFixedThreadPool(max_num_of_pools);
		 *		processes = new ArrayList<Future<Runnable>>();
         *          Future process= manager.submit(new Runnable(){
		 *			@Override
		 *			public void run() { //necessary method to be called
		 *			}				
		 *			});
		 *		processes.add(process);
		 *		}
         *#      for loop with p.get()  // this will wait for p to get completed
         *#[TODO]
         *      After using manager, make sure you shutdown : manager.shutdownNow();
         *      else if you want to use Thread
         *      
         *      ArrayList<Thread> processes = new ArrayList<Thread>();
         *      processes.add(new Thread(new Runnable(){
				@Override
				public void run() {					
				}
			}));
			p.start();
			Then, join them with 'p.join();' // wait for all ps to complete tasks
         *      */
		//__VAISHALI__09_22_2017__Dividing data into chunks
		List<KVPair[]> batches = Collections.synchronizedList(new ArrayList<KVPair[]>());
		int j=0;
		for(int i=1;i<=this.num_map_tasks;i++){
			KVPair[] batch = new KVPair[(int) Math.ceil(this.data.length/this.num_map_tasks)];
			int k=0;
			while(j <(data.length*i)/this.num_map_tasks){
				batch[k] = this.data[j];
				k++;
				j++;
			}
			batches.add(batch);
		}
		
		//System.out.println(batches.get(0)[0].toString());
		//System.out.println(batches.get(0)[1].toString());
		
		//__VAISHALI_09_23_2017__Threads to call mapTask
		ArrayList<Thread> mapThreads = new ArrayList<Thread>();
		mapThreads.add(new Thread(new Runnable(){
		
        	@Override
        	public void run() {		
        		for(int i=0;i<batches.size();i++){
        			mapTask(batches.get(i), svs.namenode_m2r);
        		}
        	}
		}));
        
        //__VAISHALI_09_23_2017__Run the threads
		for(Thread thread:mapThreads){
        	thread.start();
        }
        //__VAISHALI_09_23_2017__Wait for the threads to get finished
        for(Thread thread:mapThreads){
        	thread.join();
        }
        
		System.out.println("namenode_m2r after map tasks complete:");
		pprint(svs.namenode_m2r);
		
		/*
		 *#"send" each key-value pair to its assigned reducer by placing each 
         *#into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
		 */
		
		ArrayList<KVPair>[] to_reduce_task= new ArrayList[num_reduce_tasks];
		/*
		 *#[TODO]
         *
         *#launch the reduce tasks as a new process for each. 
         *#[TODO]
         *
         *#join the reduce tasks back
         *#[TODO]
         *
         *#print output from reducer tasks 
         *#[DONE]
		 */
		//__VAISHALI_09_23_2017__Creating list of KVPair for a particular reduce task
		for(ReducerTask tr :svs.namenode_m2r){
			if(to_reduce_task[tr.task_num]==null){
				to_reduce_task[tr.task_num] = new ArrayList<KVPair>();
			}
			to_reduce_task[tr.task_num].add(tr.kv);
		}
		
		ArrayList<Thread> reduceThreads = new ArrayList<Thread>();
		reduceThreads.add(new Thread(new Runnable(){
		
        	@Override
        	public void run() {		
        		for(int i=0;i<num_reduce_tasks;i++){
        			reduceTask(to_reduce_task[i], svs.namenode_fromR);
        		}
        	}
		}));
		
		//__VAISHALI_09_23_2017__Run the threads
		for(Thread thread:reduceThreads){
			thread.start();
		}
		//__VAISHALI_09_23_2017__Wait for the threads to get finished
		for(Thread thread:reduceThreads){
			thread.join();
		}
		
		System.out.println("namenode_m2r after reduce tasks complete:");
		pprint(svs.namenode_fromR);
		
		/*#return all key-value pairs:
         *#[DONE]
		 * 
		 */
		return svs.namenode_fromR;
	}
	
	public void pprint(List list){
		if (list.size()==0){
			System.out.println();
			return;
		}
		if (list.get(0) instanceof ReducerTask){
			ReducerTask[] arrayToPrint=Arrays.copyOf(list.toArray(), list.toArray().length, ReducerTask[].class);
			Arrays.sort(arrayToPrint);
			for(ReducerTask elem : arrayToPrint){
				System.out.println(elem.toString());
			}
		}
		
		if (list.get(0) instanceof KVPair){
			KVPair[] arrayToPrint=Arrays.copyOf(list.toArray(), list.toArray().length, KVPair[].class);
			Arrays.sort(arrayToPrint);
			for(KVPair elem : arrayToPrint){
				System.out.println(elem.toString());
			}
			
		}
		
		
	}
}