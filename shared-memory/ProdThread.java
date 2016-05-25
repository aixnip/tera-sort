import java.util.concurrent.*;
import java.io.*;

public class ProdThread implements Runnable {
	private LinkedBlockingQueue<String[]> data;
	private String filename;
	private int blocksize;
	

	public ProdThread(String filename, int blocksize, LinkedBlockingQueue<String[]> data) {
		this.filename = filename;
		this.blocksize = blocksize;
		this.data = data;
	}

	public void run(){
		BufferedReader br = null;
		String line = null;
		int pointer = 0;
		String[] block = null;
		
		try{
			br = new BufferedReader(new FileReader(filename));
		}catch(Exception e){
			e.printStackTrace();
		}
		
		try {
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//debug variable
		//int count = 0;
		while(line != null){
			if(pointer == 0){ 
				block = new String[blocksize]; //allocate a new block
			}
			block[pointer] = line;
			pointer ++;
			if(pointer == blocksize){ //this block is full
				try {
					data.put(block);
					//count++;
					block = null;
					//System.out.println("done reading 1 block:" + count);
				} catch (InterruptedException e) {
					e.printStackTrace();
					Thread.currentThread().interrupt();
					return;
				}
				
				pointer = 0; //reset pointer
				//debug  
				//break;
			}
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if(block != null){
			try {
				data.put(block);
				//count++;
				//System.out.println("done reading 1 block:" + count);
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
				return;
			}
			block = null;
		}
		
		System.out.println("done reading");
		
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
