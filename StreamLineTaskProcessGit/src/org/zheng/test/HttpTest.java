package org.zheng.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class HttpTest {
	public static void main(String args[]) throws IOException{
		BufferedReader bufReader= null;
		try {
			URL url= new URL("http://www.baidu.com");
			URLConnection con=url.openConnection();
			InputStream in=con.getInputStream();
			InputStreamReader reader= new InputStreamReader(in);
			  bufReader = new  BufferedReader(reader);
			String line=null;
			while((line=bufReader.readLine())!=null){
				
				System.out.println(line+'\n');
			}
			
		} catch ( Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if( bufReader!=null){
				 bufReader.close();
			}
		}
		
	}

}
