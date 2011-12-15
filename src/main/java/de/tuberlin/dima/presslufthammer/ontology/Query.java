package de.tuberlin.dima.presslufthammer.ontology;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * a query representation for sending in network
 */
public class Query implements Serializable {
	
	private long id;
	
	// TODO for testing the network there is no content yet
	
	public Query(long id) {
		this.id = id;
	}
	
	public long getId() {
		return id;
	}
	
	public static byte[] toByteArray(Query query) throws IOException {
		byte[] res = null;
		ByteArrayOutputStream aout = new ByteArrayOutputStream();
		ObjectOutput oout = new ObjectOutputStream(aout);
		
		oout.writeObject(query);
		
		res = aout.toByteArray();
		
		oout.close();
		aout.close();
		
		return res;
	}
	
	public static Query fromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream ain = new ByteArrayInputStream(bytes);
		ObjectInput in = new ObjectInputStream(ain);
		Object obj = in.readObject();
		
		ain.close();
		in.close();
		
		return (Query) obj;
	}
}