package hdfs;


import java.io.ObjectOutputStream;
import java.net.Socket;



public class EmetteurDN implements Runnable {

	private int port;
	private int timelapse;
	
	public EmetteurDN(int portPanne,String timelapse){
		this.timelapse = Integer.parseInt(timelapse);
		this.port= portPanne;	
	}
	
	
	@Override
	public void run() {
		Socket s;
		try {
			
			// Pour éciter des ptoblèmes de au niveau de la gestion des pannes 
			Thread.sleep(3000);
			s = new Socket(NameNode.NameNodeadresse,this.port);
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			while(true){
				Thread.sleep(timelapse*1000);
				String msg = "salut je suis lemetteur de port " + (this.port-1000);
				oos.writeObject(msg);
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
