import java.util.concurrent.*;
import java.io.*;

public class Sorting {
	protected static int blocksize = 10; //the number of search file lines per block, MUST be multiple of 10; default 10M
	protected static String filepath = "/Users/pinxiaye/Documents/workspace/553hw2/"; //default local pwd
	protected static int cthreads = 3; //number of concurrent merge threads
	
	public static void main(String[] args) {
		if(args.length != 0){
			filepath = args[0];
			try{
				cthreads = Integer.parseInt(args[1]);
				blocksize = Integer.parseInt(args[2]); //block size(MB)
			}catch(NumberFormatException nfe){
				
			}
		}
		System.out.println("Sorting with " + cthreads + " threads \nmapping blocksize " + blocksize + " MB");
		blocksize *= 10000; // change to number of lines
		
		System.out.println("Sorting with " + cthreads + " threads");
		
		File datafile = new File(filepath + "records.txt");
		long filesize = 0;
		long totalblocks = 0;
				
		if(datafile != null && datafile.exists()){
			filesize = datafile.length();
			filesize /= 100; //100 bytes per records.
		}
		
		if(filesize != 0) totalblocks = filesize/blocksize;
		
		//System.out.println("total blocks " + totalblocks);
		
		
		//make necessary directories
		for(int i = 0; i <= getHashMax(); i++){
			boolean suc = new File( filepath + i).mkdirs();
			if(!suc){
				System.out.println("can't make directory.");
				return;
			}
		}
		
		boolean suc = new File( filepath + "final").mkdirs();
		
		
		//producer thread
		LinkedBlockingQueue<String[]> lbq = new LinkedBlockingQueue<String[]>(10);
		ProdThread r = new ProdThread(filepath + "records.txt", blocksize, lbq);
		
		Thread rthread = new Thread(r);
		rthread.start();
		
		
		//consumer threads
		ExecutorService conspool = Executors.newFixedThreadPool(cthreads);
		for(int i = 0; i<totalblocks ; i++){
			conspool.execute(new ConsThread(filepath, i, lbq));
		}
		
		conspool.shutdown();
		
		try {
			conspool.awaitTermination(90000, TimeUnit.SECONDS);
			System.out.println("done sorting chucks");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		lbq = null;
		//merge files
		ExecutorService mergepool = Executors.newFixedThreadPool(cthreads);
		for(int i = 0; i <= getHashMax() ; i++){
			mergepool.execute(new MergeThread(filepath, i, (int)totalblocks, blocksize));
		}
	
		mergepool.shutdown();
		
		
		
		
		
	}
	
	//util function to hash a record.
	public static int hash(String s){
		char c1 = s.charAt(0);
		char c2 = s.charAt(1);
		
		int v1 = (int)c1;
		int v2 = (int)c2;
		
		int res = (v1 -32) * 3 + (v2 -32)/32;
		
		return res;
	}
	
	public static int getHashMax(){
		int res = (126 -32) * 3 + (126 -32)/32;
		return res;
	}

}
