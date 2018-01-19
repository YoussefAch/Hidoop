package application.montecarlo;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;
import ordo.SortComparator;

public class MonteCarlo implements MapReduce {

	private static final long serialVersionUID = 1L;

	public boolean isInRegion(String point) {
		String[] coordonnes = point.split("@");
		double x = Double.parseDouble(coordonnes[0]);
		double y = Double.parseDouble(coordonnes[1]);

		if (x * x + y * y > 0.25) {
			return false;
		}
		return true;
	}

	public void map(FormatReader reader, FormatWriter writer) {

		Map<String, Integer> hm = new HashMap<>();
		KV kv;

		while ((kv = reader.read()) != null) {
			String point = kv.v;
			if (isInRegion(point)) {
				hm.put(point, 1);
			} else {
				hm.put(point, 0);
			}
		}
		for (String k : hm.keySet())
			writer.write(new KV(k, hm.get(k).toString()));
	}

	public void reduce(FormatReader reader, FormatWriter writer) {

		KV kv;
		int sommeValeursenRegion = 0;
		int nbPoints = 0;
		while ((kv = reader.read()) != null) {
			if (Integer.parseInt(kv.v) == 1) {
				sommeValeursenRegion += 1;
			}
			nbPoints += 1;
		}
		double pi = 16 * ((double) sommeValeursenRegion / nbPoints);
		writer.write(new KV("estimation", Double.toString(pi)));
	}

	public static int getNbPoint() {
		boolean choixvalide = false;
		int choix = 0;
		while (!choixvalide) {
			System.out.println("Veuillez entrer le nombre de points souhaité :");
			Scanner sc = new Scanner(System.in);
			try {
				choix = Integer.parseInt(sc.nextLine());
				choixvalide = true;
			} catch (IllegalArgumentException e) {
				System.out.println("Veuillez entrer uniquement un entier.");
				choixvalide = false;
			}
		}
		return choix;
	}

	public static void main(String args[]) {
		Job j = new Job();
		SortComparator scomp = new ComparatorMonteCarlo();
		j.setSortComparator(scomp);
		int nbPoint = getNbPoint();
		HaltonGenerateur.generateFile("source.txt", nbPoint);
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname("source.txt");
		j.setOutputFname("pi.txt");
		j.setNumberOfReduces(1);
		long t1 = System.currentTimeMillis();
		j.startJob(new MonteCarlo());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
	}

}
