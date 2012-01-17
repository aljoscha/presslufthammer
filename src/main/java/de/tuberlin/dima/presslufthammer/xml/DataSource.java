package de.tuberlin.dima.presslufthammer.xml;

import java.util.ArrayList;
import java.util.List;

public class DataSource {
	private final String name;
	private final String protopath;
	private List<Field> fields;
	private List<String> tablets;
	
	public DataSource(String name, String ppath) {
		this.name = name;
		this.protopath = ppath;
		this.fields = new ArrayList<Field>();
		this.tablets = new ArrayList<String>();
	}
	
	public String getProtopath() {
		return protopath;
	}

	public String getName() {
		return name;
	}
	
	public int getNumFields() {
		return fields.size();
	}
	
	public int getNumTablets() {
		return tablets.size();
	}
	
	public Field getField(int i) {
		return fields.get(i);
	}
	
	public String getTablet(int i) {
		return tablets.get(i);
	}
	
	public void addField(String name, String type) {
		assert(fields != null);
		fields.add(new Field(name, type));
	}
	
	public void addTablet(String tablet) {
		assert(tablets != null);
		tablets.add(tablet);
	}

	@Override
	public String toString() {
		String string = "DataSource: " + name + "@" + protopath;
		string += "::fields:" + getNumFields() + "::tablets:" + getNumTablets();
		return string;
	}

	public class Field {
		public String name;
		public String type;
		
		public Field(String name, String type) {
			this.name = name;
			this.type = type;
		}
	}
}
