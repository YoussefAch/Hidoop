package ordo;


import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;



public class EmetteurDaemon extends Thread {

	private int portEmetteur;
	private static final int periodeHeartBeat = 10;
	
	public EmetteurDaemon (int port){
		this.portEmetteur= port;	
	}
	
	
	@Override
	public void run() {
		Socket s;
		try {
			InetAddress adrReg = InetAddress.getByName(RegistreServeur.Registreadresse);

			s = new Socket(adrReg,this.portEmetteur);
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			while(true){
				Thread.sleep(periodeHeartBeat*1000);
				int msg = this.portEmetteur-2000;
				oos.writeObject(msg);
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	
	}

}
