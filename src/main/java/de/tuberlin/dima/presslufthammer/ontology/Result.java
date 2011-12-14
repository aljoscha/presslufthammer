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
 * resposnse to an query
 */
public class Result implements Serializable {
	
	private long id;
	
	// TODO for testing the network the response is just a string with the name of the answered nodes
	private String value;
	
	public Result(long id, String value) {
		this.id = id;
		this.value = value;
	}
	
	public long getId() {
		return id;
	}
	
	public String getValue() {
		return value;
	}
	
	public static byte[] toByteArray(Result data) throws IOException {
		byte[] res = null;
		ByteArrayOutputStream aout = new ByteArrayOutputStream();
		ObjectOutput oout = new ObjectOutputStream(aout);
		
		oout.writeObject(data);
		
		res = aout.toByteArray();
		
		oout.close();
		aout.close();
		
		return res;
	}
	
	public static Result fromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream ain = new ByteArrayInputStream(bytes);
		ObjectInput in = new ObjectInputStream(ain);
		Object obj = in.readObject();
		
		ain.close();
		in.close();
		
		return (Result) obj;
	}
}