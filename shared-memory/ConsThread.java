import java.util.Arrays;
import java.util.concurrent.*;
import java.io.*;

public class ConsThread implements Runnable{
	private int id;
	private LinkedBlockingQueue<String[]> data;
	private String filepath;
	//debug: private static int counter = 0;
		
	public ConsThread(String filepath, int id, LinkedBlockingQueue<String[]> data) {
		this.id = id;
		this.filepath = filepath;
		this.data = data;
	}
	
	public void run(){
		String[] block = null;
		PrintWriter pw = null;
		
		try {
			block = data.take();
		//	counter++;
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
			return;
		}
		
		/*debug varifies data
		int countdata = 0;
		for(int i = 0; i<block.length; i++){
			if(block[i].length()>10) countdata++;
		}
		*/
		//System.out.println("took data " + block.length + " : " + counter + " varified " + countdata);
		
		
		mergeSort(block, 0, block.length-1);
		
		/*debug varifies data
		countdata = 0;
				for(int i = 0; i<block.length; i++){
					if(block[i].length()>10) countdata++;
				}
				
		System.out.println("sorted data " + block.length + " : " + counter + " varified " + countdata);
		*/
		//System.out.println("done sorting " + Arrays.toString(block));
		
		
		//start write files
		int pointer = 0;
		int fd = 0;
		while(fd <= Sorting.getHashMax() && pointer < block.length){
			File f = null;
			try{
				f = new File(filepath + fd + "/" + fd + "-" + id + ".txt");
				pw = new PrintWriter(new FileWriter(f));
			}catch(IOException ioe){
				ioe.printStackTrace();
			}
			int hashv = Sorting.hash(block[pointer]);
			while(hashv == fd && pointer < block.length){
				pw.write(block[pointer] + "\n");
				//System.out.println("wrote linecount " + pointer + " record " + block[pointer]);
				pointer++;
				if(pointer < block.length) hashv = Sorting.hash(block[pointer]);
			}
		
			pw.close();
			fd++;	
		}
		
		block = null;
		
		
	}
	
	private void mergeSort(String[] block, int start, int end){
		if(end - start > 8){
			int med = (start+end)/2;
			mergeSort(block, start, med);
			mergeSort(block, med+1, end);
			merge(block, start, med, end);
		}else{
			sort(block, start, end);
			//Debug
			//for(int i = start; i<end;i++) System.out.println(block[i]);
			
		}
	}
	
	//insertion sort the mini splitted array
	private void sort(String[] block, int start, int end){
		for(int i = start+1; i<=end; i++){
			String ckey = block[i].substring(0, 9);
			int j = start;
			while(block[j].substring(0, 10).compareTo(ckey)<=0 && j<i){
				j++;
			}
			if(j != i){
				insert(block, j, i);
			}
		}
		
	}
	
	//part of insertion sort
	private void insert(String[] block, int ins, int end){
		String inserted = block[end];
		for(int i = end; i > ins; i--){
			block[i] = block[i-1];
		}
		block[ins] = inserted;
	}
	
	//merge two subarrays
	private void merge(String[] block, int start, int med, int end){
		int l = end-start+1;
		String[] aux = new String[l];
		int pointer1 = start, pointer2 = med+1;
		for(int i = 0; i<l; i++){
			if(pointer1 > med){
				aux[i]=block[pointer2];
				pointer2++;
			}else if(pointer2>end){
				aux[i]=block[pointer1];
				pointer1++;
			}else{
				if(block[pointer1].substring(0, 10).compareTo(block[pointer2].substring(0, 10))<=0){
					aux[i]=block[pointer1];
					pointer1++;
				}else{
					aux[i]=block[pointer2];
					pointer2++;
				}
			}
		}
		for(int i = 0; i<l; i++){
			block[start+i] = aux[i];
		}
	}

	

}
