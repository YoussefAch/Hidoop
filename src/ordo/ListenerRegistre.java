package ordo;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class ListenerRegistre extends Thread {
	
	private int portListener;

	public ListenerRegistre (int port) {
		this.portListener = port;
		
	}
	
	@Override
	public void run() {
		
		try {
			ServerSocket ss = new ServerSocket(this.portListener);
			Socket s = ss.accept();
			s.setSoTimeout(25*1000);
			ObjectInputStream is = new ObjectInputStream(s.getInputStream());
			while(true){
				try{
				int message=  (int) is.readObject();
				System.out.println("heartbeat reçu serveur " +
						RegistreServeur.getNameByPortTcp(message)+ " => port "+message);
				System.out.println("nombre de serveurs fonctionnels: " + RegistreServeur.getListeserveurs().size());
				}
				catch (SocketTimeoutException | EOFException e) {
					System.out.println("serveur en panne " +RegistreServeur.getNameByPortTcp(portListener-2000));
					
					synchronized(RegistreServeur.getListeserveurs()){
						
						String nameServerToDelete = RegistreServeur.getNameByPortTcp(portListener-2000);
						RegistreServeur.getListeserveurs().remove(nameServerToDelete);
					}
					break;
				}
			}
			s.close();	
			ss.close();
		} catch (Exception e) {
			e.printStackTrace();

		}
		
	}

}
