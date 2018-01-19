package hdfs;

import java.util.ArrayList;
import java.util.List;

import formats.Format;
import ordo.CallBack;

public class LanceurWrite implements Runnable {

	private List<ArrayList<String>> listeBlocsMachines;
	private int numeroReplication;
	private long taille;
	private Format source;
	private String localFSSourceFname;
	private int DecoupageBlocs;
	private CallBack cb;

	public LanceurWrite(List<ArrayList<String>> listeBlocsMachines, int rep, long taille, Format source,
			String localFSSourceFname, int nbBlocs, CallBack cb) {
		this.listeBlocsMachines = listeBlocsMachines;
		this.numeroReplication = rep;
		this.taille = taille;
		this.source = source;
		this.localFSSourceFname = localFSSourceFname;
		this.DecoupageBlocs = nbBlocs;
		this.cb = cb;
	}

	/*
	 * public void synchronized lancerWrite() {
	 * 
	 * }
	 */

	@Override
	public void run() {

		ArrayList<String> machinesBlocsCourants = new ArrayList<String>();
		for (int k = 0; k < listeBlocsMachines.size(); k++) {
			machinesBlocsCourants.add(listeBlocsMachines.get(k).get(this.numeroReplication));
		}

		long aecr = 0; /* Nombre de byte à ecrire */
		long res = 0; /* Nombre de byte restant de l'écriture */
		int i = 0;
		for (String machine : machinesBlocsCourants) {

			String[] mach = machine.split("@");
			String adresse = mach[0];
			int port = Integer.parseInt(mach[1]);

			if ((i == DecoupageBlocs - 1) && (taille != 0)) {
				aecr = 0;
				/*
				 * Si dernier bloc, le reste du fichier doit etre écrit en
				 * entierArrayList<Integer> ports = new ArrayList<>();
				 */
			} else if ((i != DecoupageBlocs - 1) && (taille != 0)) {
				aecr = taille / DecoupageBlocs + res;
			} else {
				aecr = -1;
			}
			try {
				res = HdfsHelper.writeFileInDN(i + 1, adresse, port, localFSSourceFname, source, aecr);
			} catch (Exception e) {
				System.out.println("Opération de write non effectuée");
				break;
			}
			i++;
		}
		source.close();
		try {
			if (this.cb != null) {
			this.cb.onFinished();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
