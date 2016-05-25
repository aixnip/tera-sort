import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.nio.file.StandardCopyOption.*;

public class MergeThread implements Runnable {
	private String folderpath;
	private int foldername;
	private int nfiles;
	private int blocksize;

	public MergeThread(String folderpath, int foldername, int nfiles, int blocksize) {
		this.folderpath = folderpath;
		this.foldername = foldername;
		this.blocksize = blocksize;
		this.nfiles = nfiles;
	}

	@Override
	public void run() {

		LinkedList<String[]> data = new LinkedList<String[]>();
		int filep = 0;
		int nsblocks = 0;
		//first iteration - merge small files
		while(filep < nfiles){
			try {
				filep=read(data, filep, blocksize);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}

			while (data.size() > 1) {
				merge(data);
			}
			
			nsblocks++;

			File f = null;
			PrintWriter pw = null;
			try {
				f = new File(folderpath + foldername
					+ "/" + nsblocks + ".txt");
				pw = new PrintWriter(new FileWriter(f));
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}

			String[] allsorted = data.pollFirst();
			if(allsorted != null){
			//System.out.println(foldername + ":" + allsorted.length);
				for (int i = 0; i < allsorted.length; i++) {
					pw.write(allsorted[i] + "\n");
				}
			}

			pw.close();
		//System.out.println("merged folder " + foldername);
		}
		
		data = null;
		
		
		if(nsblocks == 1){
			Path source = FileSystems.getDefault().getPath(folderpath + foldername, "1.txt");
			Path target = FileSystems.getDefault().getPath(folderpath + "final", foldername + ".txt");
			try {
				Files.move(source, target, REPLACE_EXISTING);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return;
		}else{ //nsblocks > 1
			try{
				kmerge(nsblocks);
			}catch(IOException ioe){
				ioe.printStackTrace();
			}
		}
		
		

	}

	private int read(LinkedList<String[]> data,int start,int blocksize) throws IOException {
		BufferedReader br = null;

		// read files
		int counter = 0;
		int pointer = start;
		while (counter < blocksize && pointer < nfiles) {
			File smallf = new File(folderpath + foldername
					+ "/" + foldername + "-" + pointer + ".txt");
			br = new BufferedReader(new FileReader(smallf));

			List<String> content = new ArrayList<String>();
			String line = null;

			line = br.readLine();

			while (line != null) {
				content.add(line);
				line = br.readLine();
			}

			String[] thefile = new String[content.size()];
			counter += content.size();
			thefile = content.toArray(thefile);
			data.add(thefile);
			content = null;
			//System.out.println(foldername + "-" + i + ".txt" + " file length " + thefile.length);
			br.close();
			smallf.delete();
			pointer++;
		}
		return pointer;
	}// method read

	private void merge(LinkedList<String[]> data) {
		String[] s1 = data.pollFirst();
		String[] s2 = data.pollFirst();
		int l = s1.length + s2.length;

		String[] aux = new String[l];
		int pointer1 = 0, pointer2 = 0;
		for (int i = 0; i < l; i++) {
			if (pointer1 >= s1.length) {
				aux[i] = s2[pointer2];
				pointer2++;
			} else if (pointer2 >= s2.length) {
				aux[i] = s1[pointer1];
				pointer1++;
			} else {
				if (s1[pointer1].substring(0, 10).compareTo(
						s2[pointer2].substring(0, 10)) <= 0) {
					aux[i] = s1[pointer1];
					pointer1++;
				} else {
					aux[i] = s2[pointer2];
					pointer2++;
				}
			}
		}
		s1 = null;
		s2 = null;
		data.add(aux);
	}
	
	private void kmerge(int k) throws IOException{
		ArrayList<MergeFile> data = new ArrayList<MergeFile>();
		String path = folderpath + foldername+ "/"; 
		File finalfile = new File(folderpath +"final/" + foldername + ".txt");
		PrintWriter pw = new PrintWriter(new FileWriter(finalfile));
		StringComparator sc = new StringComparator();
		//initial read
		for(int i = 1; i<=k; i++){
			try {
				data.add(new MergeFile(path, i, blocksize/10));
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
		
		PriorityQueue<String> pq = new PriorityQueue<String>(blocksize, sc);
		boolean done = false;
		while(!done){
			String min = null;
				for(MergeFile mf : data){
					if(mf != null && mf.size()!= 0){
						ArrayList<String> nextChuck = mf.pollChuck();
						pq.addAll(nextChuck);
						nextChuck.clear();
					}else{
						mf = null;
						done = true;
					}
				}
			
				for(MergeFile mf : data){
					if(mf != null && mf.size()!= 0){
						done = false;
						String pos = mf.peek();
						if(min == null || sc.compare(min, pos)>0){
							min = pos;
						}
					}
				}
			
			if(done){
				while(pq.peek() != null){
					pw.write(pq.poll() + "\n");
				}
			}else{
			//!done, so min != null;
				while(pq.peek() != null && sc.compare(pq.peek(), min)<=0){
					pw.write(pq.poll() + "\n");
				}
			}
		}
		
		pw.close();
		pq = null;
		data = null;
		
	}
	
	private class MergeFile{
		private BufferedReader br = null;
		private ArrayList<String> data = null;
		private int size;
		
		public MergeFile(String path, int k, int size) throws IOException{
			//System.out.println(path + " " + k + " " +  size);
			this.br = new BufferedReader(new FileReader(path + k + ".txt"));
			this.data = new ArrayList<String>();
			int pointer =0;
			this.size = size;
			String line = br.readLine();

			while (line != null && pointer < size) {
				data.add(line);
				pointer++;
				if(pointer < size)
					line = br.readLine();
			}
			
		}
		
		public String peek() throws IOException{
			if(data.size() == 0){ //load more data
				int pointer =0;
				String line = br.readLine();

				while (line != null && pointer < size) {
					data.add(line);
					pointer++;
					if(pointer < size)
						line = br.readLine();
				}
			}
			if(data.size() >0)
				return data.get(0);
			else
				return null;
		}
		
		public int size() throws IOException{
			if(data.size() == 0){ //load more data
				int pointer =0;
				String line = br.readLine();

				while (line != null && pointer < size) {
					data.add(line);
					pointer++;
					if(pointer < size)
						line = br.readLine();
				}
			}
			return data.size();
		}
		
		public ArrayList<String> pollChuck() throws IOException{
			if(data.size() == 0){ //load more data
				int pointer =0;
				String line = br.readLine();

				while (line != null && pointer < size) {
					data.add(line);
					pointer++;
					if(pointer < size)
						line = br.readLine();
				}
			}
			if(data.size() == 0)
				return null;
			else
				return this.data;
		}
		
		public void close() throws IOException{
			this.br.close();
		}
		
		
		
	}
	
	private class StringComparator implements Comparator<String>{
		
		public StringComparator(){}

		public int compare(String o1, String o2) {
			return o1.substring(0, 10).compareTo(o2.substring(0, 10));
		}
		
	}
	
	
	
	
}







