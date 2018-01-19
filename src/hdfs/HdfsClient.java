/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import javafx.util.Pair;
import ordo.CallBack;
import ordo.CallBackImpl;


public class HdfsClient {

	public static Map<Integer, String> listeMachinesMAJ = new HashMap<Integer, String>();
	
	

	private static void ShellUsage() {
		System.out.println("exit : quitter le shell");
		System.out.println("help : afficher l'aide");
		System.out.println("mv <line|kv> <file> <filename> : renommer le fichier ");
		System.out.println("cp <line|kv> <file> <filename> : copier le fichier ");
		System.out.println("rm <file> : supprimer le fichier ");
	}

	public static void HdfsDelete(String hdfsFname, int repFactor, int nbBlocs) {
		/* Récupération du noeud du fichier */
		Pair<INode,Map<Integer, String>> paire = HdfsHelper.getInode("CMD_DELETE", hdfsFname, repFactor,nbBlocs);
		INode node = paire.getKey();
		Map<Integer, String> machinesFonctionnelles = paire.getValue();
		if (node != null) {
			Map<Integer, ArrayList<String>> mapnode = node.getMapNode();
			
			/* Suppresion des blocs en contactant le dataNode adéquat */
			for (int i = 1; i <= nbBlocs; i++) {
				for(String machine : mapnode.get(i)) {
					String[] mach = machine.split("@");
					String adresse = mach[0];
					int port = Integer.parseInt(mach[1]);
					if(machinesFonctionnelles.keySet().contains(port)) {
						HdfsHelper.deleteBloc(i, adresse, port, hdfsFname);
					}
				}
			}
		} else {
			System.out.println("Erreur : Fichier inexistant.");
		}
	}

	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor, int nbBlocs) {
		Format source;
		long taille;
		List<CallBack> cbs = new LinkedList<CallBack>();
		 
		/* Test existence du fichier */
		if (new File(localFSSourceFname).exists()) {
			/* Récupération de la liste des dataNode */
			List<ArrayList<String>> listeBlocsMachines = HdfsHelper.getDataNode("CMD_WRITE", localFSSourceFname, repFactor, nbBlocs);

			/* Création de l' Executor service */
	        ExecutorService executeurWrite = Executors.newFixedThreadPool(repFactor);
			/* Envoi des données aux DataNode */
			for(int l = 0; l < repFactor; l++) {
				/* Création du fichier dans son format adéquat 
				 * Pour créer une source pour chaque Thread
				 * comme ca on évite les problèmes de sychro
				 */
				if (fmt == Format.Type.LINE) {
					source = new LineFormat(localFSSourceFname);
					taille = ((LineFormat) source).getLength();
				} else {
					source = new KVFormat(localFSSourceFname);
					taille = ((KVFormat) source).getLength();
				}
				/*Céation de l'objet callback*/
				CallBack cb;
				try {
					cb = new CallBackImpl();
					cbs.add(cb);
					executeurWrite.execute(new LanceurWrite(listeBlocsMachines, l, taille, source, localFSSourceFname, nbBlocs,cb));
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
			/*Récéption des callBack*/
			for (CallBack c : cbs) {
				try {
					((CallBackImpl) c).getCalled().acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			
			System.out.println("Opération write fini");
		} else {
			System.out.println("Erreur : Fichier inexistant.");
		}
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname, int repFactor, int nbBlocs) throws PanneDataNodeException {
		/* récupération du noeud du fichier */
		/* il contient le mapNode qui contient pour chaque bloc une liste de machines */
		
		
		
		Pair<INode, Map<Integer, String>>  paire = HdfsHelper.getInode("CMD_READ", hdfsFname, repFactor, nbBlocs);
		Format resultat = null;
				
		
		try {
			if (paire.getKey() != null) {
				int blocCourant=1;
				ArrayList<String> adressesNodes = null;
				Map<Integer, String> listemachinesFonctionnelles;
				while (blocCourant <= nbBlocs) {
					
					/* Récupération de l'adresse du premier data Node */
					adressesNodes = paire.getKey().getMapNode().get(blocCourant);
					listemachinesFonctionnelles = paire.getValue();
	
					//System.out.println("Tous les datanodes associés sont en panne");
					int compteur = 0;
					/* parcours des machines associées au bloc courant */
					for (String machine : adressesNodes) {
						
						String[] mach = machine.split("@");
						String adresse = mach[0];
						int port = Integer.parseInt(mach[1]);
						
					
						if  (!listemachinesFonctionnelles.keySet().contains(port)){
							compteur++;
							continue;
						}
						
						else {
							resultat = HdfsHelper.readFileFromDN(blocCourant, adresse, port, hdfsFname, resultat,
									localFSDestFname);
							blocCourant++;
							break;
						}
						
					}
					if (compteur >= repFactor) {
						throw new PanneDataNodeException("Tous les datanodes associés sont en panne");
					}
					
	
					
				}
				resultat.close();
			}
	
				/* Fermeture du fichier */
			else {
				System.out.println("Erreur : fichier inexistant.");
			}
		} catch (PanneDataNodeException ee) {
			System.out.println("Un problème s est produit lors de l operation read");
		}
	}

	/** Methode pour implémenter la verison shell de Hdfs */
	public static void HdfsShell(int facteurRep, int nbBlocs) {
		ShellUsage();
		Scanner sc = new Scanner(System.in);
		Boolean Nonsortir = true;
		while (Nonsortir) {
			String cmd = sc.nextLine();
			String[] cmdsplit = cmd.split(" ");
			try {
				switch (cmdsplit[0]) {
				case "exit":
					Nonsortir = false;
					break;
				case "help":
					ShellUsage();
					break;
				case "rm":
					HdfsDelete(cmdsplit[1], facteurRep, nbBlocs);
					break;
				case "cp":
					try {
						HdfsRead(cmdsplit[2], cmdsplit[3], facteurRep, nbBlocs);
					} catch (PanneDataNodeException e) {
						e.printStackTrace();
					}
					if (cmdsplit[1].equals("kv")) {
						HdfsWrite(Format.Type.KV, cmdsplit[3], facteurRep, nbBlocs);
					} else {
						HdfsWrite(Format.Type.LINE, cmdsplit[3], facteurRep, nbBlocs);
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					File f1 = new File(cmdsplit[3]);
					f1.delete();
					break;
				case "mv":
					try {
						HdfsRead(cmdsplit[2], cmdsplit[3], facteurRep, nbBlocs);
					} catch (PanneDataNodeException e) {
						e.printStackTrace();
					}
					if (cmdsplit[1].equals("kv")) {
						HdfsWrite(Format.Type.KV, cmdsplit[3], facteurRep, nbBlocs);
					} else {
						HdfsWrite(Format.Type.LINE, cmdsplit[3], facteurRep, nbBlocs);
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					File f2 = new File(cmdsplit[3]);
					f2.delete();
					HdfsDelete(cmdsplit[2], facteurRep, nbBlocs);
					break;
				default:
					System.out.println("Commande inconnue");
					break;
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				ShellUsage();
			}
		}
	}


	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>
		Scanner sc1 = new Scanner(System.in);
		System.out.println("-------------------------------------------------------");
		System.out.println("-------------------------------------------------------");
		System.out.println("Bienvenue sur la plateforme HDFS V1");
		System.out.println("-------------------------------------------------------");
		System.out.println("-------------------------------------------------------");
		System.out.println("Veuillez choisir le facteur de réplication pour vos fichiers");
		int facteurRep = sc1.nextInt();
		System.out.println("Veuillez choisir le nombre de blocs à créer pour vos fichiers");
		int nbBlocs = sc1.nextInt();
		
	
		Scanner sc2 = new Scanner(System.in);
		do {
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			
			System.out.println("Veuillez saisir le nom de votre fichier");
			String nameFile = sc2.nextLine();
			System.out.println("Veuillez choisir le format de votre fichier (kv/line)");
			String format = sc2.nextLine();
			try {
			
				System.out.println("Veuillez choisir une commande (write/read/delete)");
				String commande = sc2.nextLine();
				switch (commande) {
				case "read":
					HdfsRead(nameFile, "ReSuLtAt.txt", facteurRep, nbBlocs);
					break;
				case "delete":
					HdfsDelete(nameFile, facteurRep, nbBlocs);
					break;
				case "write":
					Format.Type fmt;
					if (format.equals("line"))
						fmt = Format.Type.LINE;
					else if (format.equals("kv"))
						fmt = Format.Type.KV;
					else {
						return;
					}
					HdfsWrite(fmt, nameFile, facteurRep, nbBlocs);
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println("Voulez-vous lancer le shell HDFS ? (oui/non)");
			String shell = sc2.nextLine();
			if (shell.equals("oui")) {
				HdfsShell(facteurRep, nbBlocs);
			}
		} while(true);
		
		
	}

}
