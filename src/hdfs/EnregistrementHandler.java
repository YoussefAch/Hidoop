package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class EnregistrementHandler implements Runnable {

	private Socket s;
	private int i;
	
	public EnregistrementHandler(Socket s, int indiceMachine) {
		this.s = s;
		this.i = indiceMachine;
	}
	
	
	@Override
	public void run() {
		try {
			/*Récupération des objets de lecture et d'ecriture*/
			ObjectInputStream br = new ObjectInputStream(s.getInputStream());
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			
			/*Reception de l'adresse de la machine.*/				
			String adresse = (String) br.readObject();
			
			/*Création du numéro de port à utiliser*/
			int port = 4501+i;
			int portPanne = 1000+port;
			
			
			/* ------ PROTOCOLE SYSTOLIQUE
			 * lancement du listener des messages du DataNode associé
			 */
			ListenerDN listener = new ListenerDN(portPanne);
			new Thread(listener).start();
			
			
			/*Envoi du numéro de port.*/
			bw.writeObject(Integer.toString(port));
			
			/*Ajout du dataNode à la liste*/
			synchronized(NameNode.listemachines) {
				NameNode.listemachines.put(port, adresse);
			}
			br.close();
			bw.close();
			s.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
