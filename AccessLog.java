package br.com.test.spark.nasa;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLog implements Serializable
{
	private static final Logger logger = Logger.getLogger("Access");

	private String ipAddress;
	private String dateTimeString;
	private String endpoint;
	private int responseCode;
	private long contentSize;


	private AccessLog(String ipAddress,String dateTime, String endpoint,
			String responseCode, String contentSize)
	{
		this.ipAddress = ipAddress;
		this.dateTimeString = dateTime;
		this.endpoint = endpoint;
		this.responseCode = Integer.parseInt(responseCode.equals("")?"0":responseCode);
		if (contentSize.contains("-")  || contentSize.contains(""))
		{
			this.contentSize = 0L;
		} else
		{
			this.contentSize = Long.parseLong(contentSize);
		}

	}

	public String getIpAddress()
	{
		return ipAddress;
	}


	public String getDateTimeString()
	{
		return dateTimeString;
	}


	public String getEndpoint()
	{
		return endpoint;
	}


	public int getResponseCode()
	{
		return responseCode;
	}

	public long getContentSize()
	{
		return contentSize;
	}

	public void setIpAddress(String ipAddress)
	{
		this.ipAddress = ipAddress;
	}


	public void setDateTimeString(String dateTimeString)
	{
		this.dateTimeString = dateTimeString;
	}


	public void setEndpoint(String endpoint)
	{
		this.endpoint = endpoint;
	}


	public void setResponseCode(int responseCode)
	{
		this.responseCode = responseCode;
	}

	public void setContentSize(long contentSize)
	{
		this.contentSize = contentSize;
	}
	// 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/
	// HTTP/1.0" 200 6245
	private static final String LOG_ENTRY_PATTERN =
			// 1:IP 2:client 3:user 4:date time 5:method 6:req 7:proto
			// 8:respcode 9:size
			"^(.*) - - \\[(.*):(.*):(.*):(.*)\\] \"(.*)\" ([0-9]*) ([0-9]*|-)";
	private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

	public static AccessLog parseFromLogLine(String logline)
	{
		Matcher m = PATTERN.matcher(logline);
		if (!m.find())
		{
				System.out.println(logline);
				return new AccessLog("","","","0", "0");
			
		}

		return new AccessLog(m.group(1), m.group(2), m.group(6), m.group(7),
				m.group(8) );

	}

	@Override
	public String toString()
	{
		return String.format("%s [%s] \"%s\" %s %s", ipAddress,
				 dateTimeString, endpoint,
				responseCode, contentSize);
	}
	public static void main(String[] args)
	{
		
		//Criar arquivo com o resultado:
        try
		{
			PrintWriter out = new PrintWriter("C:\\temp\\saida.txt");
			out.println("Total de registros: "+1);
			out.println("hosts unicos: " + 2);
			out.println("Total de erros 404: "+3);
			out.println();
			out.println();
			out.close();
		} catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		String s = "204.120.229.63 - - [01/Jul/1995:04:29:05 -0400] \"GET /history/history.html                                                 hqpao/hqpao_home.html HTTP/1.0\" 200 1502";
		String s2 = "htu.ero.ornl.gov - - [05/Jul/1995:08:54:32 -0400] \"GET /shuttle/missions/sts-70/images/woodpecker-on-et.gif HTTP/1.0\" 200 7318";
		
		Pattern p1 = Pattern.compile("^\\[(.*?)\\]$");
		Matcher m1 = p1.matcher(s);
		
		if(m1.find()) {
			m1.groupCount();
		}
		
		System.out.println(s.split("-")[0]);
		
		
		String pattern = "^(\\S+) (\\S+) (\\S+) " +  
         "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +  
         " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";
		
		String pattern2 = "^(.*) - - \\[(.*):(.*):(.*):(.*)\\] \"(.*)\" ([0-9]*) ([0-9]*|-)";
		Pattern p = Pattern.compile(pattern2);
		
		Matcher m = p.matcher(s);
		if(m.find()) {
			System.out.println(m.group(0));
			System.out.println(m.group(1));
			System.out.println(m.group(2));
			System.out.println(m.group(3));
			System.out.println(m.group(4));
			System.out.println(m.group(5));
			System.out.println(m.group(6));
			System.out.println(m.group(7));
			System.out.println(m.group(8));
			
		};
	}
}
