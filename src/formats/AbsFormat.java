package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public abstract class AbsFormat implements Format {


	private static final long serialVersionUID = 1L;
	private String fileName;
	protected boolean openR;
	protected boolean openW;
	private FileReader fr;
	private FileWriter fw;
	protected BufferedReader br;
	protected BufferedWriter bw;
	protected File file;
	protected int index;

	public AbsFormat(String name) {
		this.fileName = name;
		this.file = new File(name);
		this.index = 0;
	}

	@Override
	public void open(OpenMode mode) {
		try {
			if (mode == Format.OpenMode.R) {
				this.fr = new FileReader(this.file);
				this.br = new BufferedReader(fr);
				this.openR = true;
			}

			if (mode == Format.OpenMode.W) {
				this.fw = new FileWriter(this.file);
				this.bw = new BufferedWriter(this.fw);
				this.openW = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
		if (this.openR) {
			this.openR = false;
			this.br.close();
			this.fr.close();
		}
		if (this.openW) {
			this.openW = false;
			this.bw.close();
			this.fw.close();
		}
		this.index = 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.fileName;
	}

	@Override
	public void setFname(String fname) {
		this.fileName = fname;
	}
	
	public long getLength() {
		return this.file.length();
	}
	
	public void delete() {
		this.file.delete();
	}
	
	public abstract String getType();
}
