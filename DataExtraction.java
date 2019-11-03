package br.com.test.spark.nasa;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DataExtraction
{
	public static void main(String[] args)
	{
		// configuração do Spark
		SparkConf conf = new SparkConf().setMaster("local")
				.setAppName("BusProcessor");
		JavaSparkContext ctx = new JavaSparkContext(conf);

		// carrega os dados do trace da NASA Jul e Ago
		JavaRDD<String> logsJul = ctx.textFile("c:/temp/access_log_Jul95");
		JavaRDD<String> logsAgo = ctx.textFile("c:/temp/access_log_Aug95");

		// Unindo os dataSets
		JavaRDD<String> linhas = logsJul.union(logsAgo);

		// Convertendo as linhas do log para o objeto AccessLog e criando um
		// cache
		// para facilitar a manipulação desses dados.
		JavaRDD<AccessLog> accessLogs = linhas.map(AccessLog::parseFromLogLine)
				.cache();

		System.out.println("Total de registros: " + accessLogs.count());

		// Precisamos de saber quais os IP`s
		// Então fiz outro map para recuperar apenas os IP`s, depois fiz um
		// distinct count para saber quais os hosts
		// únicos
		JavaRDD<String> ips = accessLogs.map(f -> f.getIpAddress());
		long num_hosts_unicos = ips.distinct().count();

		System.out.println("hosts unicos: " + num_hosts_unicos);

		// Criar filtro nos logs para filtrar os códigos 404
		JavaRDD<AccessLog> error404 = accessLogs
				.filter(f -> f.getResponseCode() == 404);
		long total_404 = error404.count();
		System.out.println("Total de erros 404: " + total_404);

		// Temos que transformar o RDD em um objeto Chave, Valor, para realizar
		// a contagem
		// Utilizamos o método reduceByKey para agrupar os registros iguais
		// agregando a quantidade de ocorrências
		// depois fazemos o sort descendente para ordenar os top 5
		JavaPairRDD<String, Integer> pairsTopUrls = error404
				.mapToPair(w -> new Tuple2(w.getEndpoint(), 1));
		JavaPairRDD<String, Integer> counts = pairsTopUrls
				.reduceByKey((x, y) -> x + y).sortByKey(false);
		List<String> topUrls = counts.map(f -> f._1()).take(5);

		// Percorre a lista para imprimir
		System.out.println("5 urls com  mais erros:");
		for (String string : topUrls)
		{
			System.out.println(string);
		}

		// Mesmo procedimento feito para as URLS mas agora sem fazer o sort
		// descendente para todas as datas
		JavaPairRDD<String, Integer> pairFalhasPorDia = error404
				.mapToPair(w -> new Tuple2(w.getDateTimeString(), 1));
		JavaPairRDD<String, Integer> countsFalhasPorDia = pairFalhasPorDia
				.reduceByKey((x, y) -> x + y).sortByKey();

		// método collect retorna a lista.
		List<Tuple2<String, Integer>> errosPorDia = countsFalhasPorDia
				.collect();

		// Percorre a Tupla para imprimir
		System.out.println("Erros por dia:");
		for (Tuple2<String, Integer> erro : errosPorDia)
		{
			System.out.println(erro._1() + " - " + erro._2());
		}

		// Criar arquivo com o resultado:
		try
		{
			PrintWriter out = new PrintWriter("C:\\temp\\saida.txt");
			out.println("Total de registros: " + accessLogs.count());
			out.println("hosts unicos: " + num_hosts_unicos);
			out.println("Total de erros 404: " + total_404);
			out.println("5 urls com  mais erros:");
			for (String string : topUrls)
			{
				out.println(string);
			}
			out.println("Erros por dia:");
			for (Tuple2<String, Integer> erro : errosPorDia)
			{
				out.println(erro._1() + " - " + erro._2());
			}
			out.println();
			out.close();
		} catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ctx.close();
	}

}
