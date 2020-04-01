package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.columnar.NULL;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import static org.apache.spark.sql.functions.col;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		List<Row> vertices_list = new ArrayList<Row>();
		//load the vertices
		try {
			BufferedReader wikiVerticesReader = new BufferedReader(
					//find a better way to load resource
					new FileReader("/Users/akashmalhotra/Documents/SDM/SparkGraphXassignment/src/main/resources/wiki-vertices.txt")
			);
			//String[] content;
			String line;
			while ((line = wikiVerticesReader.readLine()) != null) {
				String[] tokens = line.split("\\s+", 2);
				//System.out.println(tokens[0] + " " + tokens[1]);
				vertices_list.add(RowFactory.create(tokens[0], tokens[1]));
			}
		}catch(Exception e){
			System.out.println(e);
		}
		//create the vertices
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
		//load the edges
		List<Row> edges_list = new ArrayList<Row>();
		try {
			BufferedReader wikiEdgesReader = new BufferedReader(
					//find a better way to load resource, otherwise modify the local path
					new FileReader("/Users/akashmalhotra/Documents/SDM/SparkGraphXassignment/src/main/resources/wiki-edges.txt")
			);
			String line;
			while ((line = wikiEdgesReader.readLine()) != null) {
				String[] tokens = line.split("\\s+", 2);
				//System.out.println(tokens[0] + " " + tokens[1]);
				edges_list.add(RowFactory.create(tokens[0], tokens[1]));
			}
		}catch(Exception e){
			System.out.println(e);
		}
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
				//new StructField("relationship", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

		System.out.println(gf);

		gf.edges().show();
		gf.vertices().show();


		System.out.println("PageRank algorithm: ");
		//tune maximum iterations
		/*
		int i = 1;
		Dataset<Row> previousVertices =
				gf.pageRank()
						.maxIter(i)
						.resetProbability(0.15) //damping factor = 1-resetProbability
						.run()
						.vertices()
						.orderBy(col("pagerank").desc())
						.limit(10);

		Dataset<Row> currentVertices;
		for(i = 2; i < 20; i++) {
			System.out.println("This iteration: "+ i);
			currentVertices =
					gf.pageRank()
					.maxIter(i)
					.resetProbability(0.15) //damping factor = 1-resetProbability
					.run()
					.vertices()
					.orderBy(col("pagerank").desc())
					.limit(10);
			currentVertices.show();
			if(currentVertices.select("id").toJavaRDD().collect().equals(previousVertices.select("id").toJavaRDD().collect())) {
				System.out.println("Optimum iterations for PageRank : "+i);
				currentVertices.show();
				break;
			}else{
				previousVertices = currentVertices;
			}
		}*/
		//tune resetProbability

		int maxIterations = 11;
		double resetProb = 0.70;
		for(int i =0; i < 7; i++){
			java.util.List<Row> distribution = gf.pageRank()
					.maxIter(maxIterations)
					.resetProbability(resetProb) //damping factor = 1-resetProbability
					.run()
					.vertices()
					.select("pagerank").toJavaRDD().collect();
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter("distribution" + resetProb + ".txt"));
				Iterator<Row> iterator = distribution.iterator();
				while (iterator.hasNext()) {
					bw.append(iterator.next().get(0).toString());
					bw.newLine();
				}
				bw.flush();
				bw.close();
			}catch (Exception e){
				System.out.println(e.getStackTrace());
			}


		}



	}

}
