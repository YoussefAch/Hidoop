package ordo;

import java.io.IOException;
import java.util.Scanner;

import config.Project;
import formats.Format.Type;
import hdfs.HdfsClient;

public class HidoopClient {

	public static void main(String[] args) {
		
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Veuillez saisir le nom de l'application à executer sur la plateforme : ");
		String app = sc.nextLine();
		System.out.println("Veuillez saisir le nom du fichier à traiter avec le (.txt) : ");
		String nameFile = sc.nextLine();
		System.out.println("Veuillez saisir le type du fichier à traiter (kv ou line) : "); 
		String typeFile = sc.nextLine();
		
		String[] commandeWrite = {"java", "hdfs.HdfsClient", "write", "kv", nameFile};
		if (typeFile.equals("line")) {
			commandeWrite[3] = "line";
		} 
		
		try {
			System.out.println("lancement Write");
			Process write = Runtime.getRuntime().exec(commandeWrite);
			write.waitFor();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		
		/* Lancement des 3 s*/
		int numServeur = -1;
		Process p= null;
		for (String nomServeur : Project.listeServeurs.keySet()) {
			numServeur++;
			System.out.println("Execution du serveur "+numServeur);
			String portServeur = Integer.toString(Project.listeServeurs.get(nomServeur));
			String[] commande = {"java", "ordo.DaemonImpl", portServeur, nomServeur};
			try {
				Runtime.getRuntime().exec(commande);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		
		/* Lancement de l'appliction*/
		System.out.println("Execution de "+app);
		String[] commandeApp = {"java", "application."+app, nameFile};
		try {
			p = Runtime.getRuntime().exec(commandeApp);
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	

}
