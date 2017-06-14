package udfHive;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class WordCount extends UDF{

	public int evaluate(Text text){
		int count=0;
		if (text==null) return 0;
		
		StringTokenizer str = new StringTokenizer(text.toString());
		
		while(str.hasMoreTokens()){
			str.nextToken();
			count++;
		}
		return count;
		
	}
	

}
