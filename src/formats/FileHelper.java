package formats;

import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class FileHelper {

	/**
	 * Méthode qui lie sur un Format et écrit sur le ObjectWriter passé en
	 * paramètre.
	 * 
	 * @param f
	 *            : fichier format
	 * @param ois
	 *            : object writer
	 * @param nbbyte
	 *            : nombre total de byte à écrire
	 * @return nombre de byte restant quand nbbyte = 0 tout est lu et écrit
	 *         quand nbbyte = -1, rien n'est écrit.
	 */

	public static long readFile(Format f, ObjectOutputStream oos, long nbbyte) {
		long nblu = 0;
		long nbres = nbbyte;
		Boolean continuer = false;
		KV recordactu = null;
		try {
			if (nbbyte != -1) {
				recordactu = f.read();
				continuer = true;
				while ((recordactu != null) && (continuer)) {
					/* Calcul du nombre de byte lu */
					if (f instanceof KVFormat) {
						nblu = (recordactu.k + KV.SEPARATOR + recordactu.v).getBytes().length;
					} else {
						nblu = (recordactu.v).getBytes().length;
					}
					/* Mise a jour de continuer */
					if (nbbyte == 0) {
						continuer = true;
					} else {
						if (nblu < nbres) {
							continuer = true;
							nbres = nbres - nblu;
						} else {
							continuer = false;
							nbres = nbres - nblu;
						}
					}
					/* Ecriture et mise a jour de record actu */
					oos.writeObject(recordactu);
					if (continuer) {
						recordactu = f.read();
					}
				}
			} if (recordactu == null) {
				/*Envoie d'un record null en cas de fichier vide.*/
				oos.writeObject(recordactu);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return nbres;
	}

	/**
	 * Méthode qui lie sur un ObjectReader et écrit sur le Format passé en
	 * paramètre.
	 * 
	 * @param f
	 *            : fichier format
	 * @param oos
	 *            : l'objectReader
	 */
	public static void writeFile(Format f, ObjectInputStream ois) {
		Boolean continuer = true;
		try {
			KV recordactu = (KV) ois.readObject();
			while ((recordactu != null) && (continuer)) {
				/* Ecriture dans le fichier */
				f.write(recordactu);
				try {
					recordactu = (KV) ois.readObject();
				} catch (EOFException e) {
					/* Tout a été lu */
					continuer = false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
