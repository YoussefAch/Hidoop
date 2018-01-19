package hdfs;

import java.io.EOFException;
import java.io.ObjectInputStream;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Scanner;



public class ListenerDN implements Runnable {

	private int portPanne;
	
	public ListenerDN (int portPanne) {
		this.portPanne = portPanne;
		
	}
	
	
	
	/*
	 * Si on reçoit pas de notif au bout de 25 secondes on considère que le datanode est en panne.
	 * 
	 */
	@Override
	public void run() {
		
		long t1;
		long t2;
		//Socket s;
		try {
			
			int nblus;
			ServerSocket ss = new ServerSocket(this.portPanne);
			Socket s = ss.accept();
			s.setSoTimeout(25*1000);
			ObjectInputStream is = new ObjectInputStream(s.getInputStream());
			while(true){
				try{
				
				String message =  (String) is.readObject();
				//System.out.println("message received : " + message);
				//System.out.println("Nombre de machines fonctionnelles : " + NameNode.listemachines.size());
				}
				catch (SocketTimeoutException | EOFException e) {
					// PANNE DETECTEE
					System.out.println("machine non fonctionnelle " + (portPanne-1000));
					System.out.println("Voulez-vous remplacer la machine non fonctionnelle (oui/non) ");
					
					// Recuperation du nombre de machines fonctionnelles
					int ancienSizeMachines;
					synchronized(NameNode.listemachines){
						ancienSizeMachines = NameNode.listemachines.size();
					}
					int nouveauSizeMachines = ancienSizeMachines;
					Scanner sc = new Scanner(System.in);
					String reponse = sc.nextLine();
					switch (reponse) {
					
						case "oui":
							System.out.println("Veuillez lancer une nouvelle machine");
							// Detecter une nouvelle machine : on la considère auto la machine qui remplace
							// le datanode en panne
							
							while (nouveauSizeMachines == ancienSizeMachines) {
								synchronized(NameNode.listemachines){
									nouveauSizeMachines = NameNode.listemachines.size();
								}
							}
							synchronized(NameNode.listemachines){
								// Supprimer la machine de la liste des machines 
								NameNode.listemachines.remove(portPanne - 1000);
								GestionnairePanne.handler(portPanne - 1000);
							}
							break;
							
							
						case "non":
							System.out.println("Vous avez choisi de ne pas remplacer la machine en panne");
							break;
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
