package application.montecarlo;

import java.io.File;
import java.io.FileWriter;

public class HaltonGenerateur {

	private static void usage() {
		System.out.println("Usage: java HaltonGenerator <filename> nbPoints");
	}

	public static void main(String args[]) {

		if (args.length < 2) {
			usage();
		}
		String filename = args[0];
		int nbPoints = Integer.parseInt(args[1]);
		generateFile(filename, nbPoints);
	}

	public static void generateFile(String filename, int nbPoints) {
		try {
			Halton halton = new Halton(1);
			System.out.println(halton.getX()[0] + " @ " + halton.getX()[1]);
			double[] point = halton.nextPoint();
			File f = new File(filename);
			FileWriter fr = new FileWriter(f);
			System.out.println(point[0] + "@" + point[1] + "\n");
			fr.write(point[0] + "@" + point[1] + "\n");

			for (int i = 0; i < nbPoints; i++) {
				point = halton.nextPoint();
				fr.write(point[0] + "@" + point[1] + "\n");

			}
			fr.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}