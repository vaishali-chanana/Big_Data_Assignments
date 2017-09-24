import java.util.concurrent.*;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;

public class Test {
	public static void main(String[] args) throws ExecutionException, InterruptedException{
		
		KVPair[] data = {new KVPair(1, "The horse raced past the barn fell"),
		                 new KVPair(2, "The complex houses married and single soldiers and their families"),
		                 new KVPair(3, "There is nothing either good or bad, but thinking makes it so"),
		                 new KVPair(4, "I burn, I pine, I perish"),
		                 new KVPair(5, "Come what come may, time and the hour runs through the roughest day"),
		                 new KVPair(6, "Be a yardstick of quality."),
		                 new KVPair(7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
		                 new KVPair(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted.")};
		                
		WordCountMR mrObject = new WordCountMR(data,4,3);
		mrObject.runSystem();
		
		//####################
	    //##run SetDifference
	    //#(TODO: uncomment when ready to test)
	    System.out.println("\n\n*****************\n Set Difference\n*****************\n");
	    KVPair[] data1 = {new KVPair('R', new String[]{"apple", "orange", "pear", "blueberry"}),	    		
	    new KVPair('S', new String[]{"pear", "orange", "strawberry", "fig", "tangerine"})};
	    
	    Random randInt = new Random();
	    ArrayList<Integer> rElems= new ArrayList<Integer>();
	    ArrayList<Integer> sElems= new ArrayList<Integer>();
	    for(int i=0;i<50;i++){
	    	if(randInt.nextInt(100) > 50){
	    		rElems.add(i);
	    	}
	    	if(randInt.nextInt(100) > 75){
	    		sElems.add(i);
	    	}
	    }
	    //int[] rSet=new int[rElems.size()];
	    //int[] sSet=new int[sElems.size()];
	    
	    // __VAISHALI_09_24_2017__Making changes to framework code as there is an error
	    int[] rSet = new int[rElems.size()];
	    for (int i=0; i < rSet.length; i++)
	    {
	        rSet[i] = rElems.get(i).intValue();
	    }
	    
	    int[] sSet = new int[sElems.size()];
	    for (int i=0; i < sSet.length; i++)
	    {
	        sSet[i] = sElems.get(i).intValue();
	    }
	    System.out.println("----");
	    System.out.println(Arrays.toString(rSet));
	    System.out.println(Arrays.toString(sSet));
		KVPair[] data2 = {new KVPair('R', rSet),
		 		 new KVPair('S', sSet)};
	    SetDifferenceMR smrObject = new SetDifferenceMR(data1, 2, 2);
	    smrObject.runSystem();
	    smrObject = new SetDifferenceMR(data2, 2, 2);
	    smrObject.runSystem();
	}
}
