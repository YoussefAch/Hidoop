package hdfs;

import formats.AbsFormat;
import formats.Format;

public class Bloc  {

	private int id;
	private Format.Type type;
	private String sourceName;
	private Format file;

	public Format.Type getType() {
		return type;
	}

	public int getId() {
		return id;
	}

	public Bloc(int id, String name, Format file, Format.Type type) {
		this.id = id;
		this.sourceName = name;
		this.file = file;
		this.type = type;
	}

	public String getSourceName() {
		return sourceName;
	}
	
	public void deleteFile() {
		((AbsFormat) this.file).delete();
	}
	public Format getFile() {
		return file;
	}
}
