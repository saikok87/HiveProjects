package com.test.hive.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(
	name="SimpleUDFEx",
	value="returns 'hello x', where x is whatever you give it (STRING)",
	extended="SELECT SimpleUDFEx('world') from foo LIMIT 1;"
	)
public class SimpleUDFEx extends UDF {

	public Text evaluate(Text input) {
		
		if(input==null) return null;
		return new Text("Hello " + input);
	}

}
