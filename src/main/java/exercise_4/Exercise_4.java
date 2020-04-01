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
					//How to load resource in a relative way?
					new FileReader("target/classes/wiki-vertices.txt")
			);
			String line;
			while ((line = wikiVerticesReader.readLine()) != null) {
				String[] tokens = line.split("\\s+", 2);
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
					new FileReader("target/classes/wiki-edges.txt")
			);
			String line;
			while ((line = wikiEdgesReader.readLine()) != null) {
				String[] tokens = line.split("\\s+", 2);
				edges_list.add(RowFactory.create(tokens[0], tokens[1]));
			}
		}catch(Exception e){
			System.out.println(e);
		}
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		//create the graphframe
		GraphFrame gf = GraphFrame.apply(vertices,edges);
		System.out.println(gf);
		gf.edges().show();
		gf.vertices().show();


		System.out.println("PageRank algorithm: ");
		//tune maximum iterations
		Dataset<Row> previousVertices = pageRank(gf,1,0.15).orderBy(col("pagerank").desc()).limit(10);
		previousVertices.show();
		Dataset<Row> currentVertices;
		for(int i = 2; i < 20; i++) {
			System.out.println("This iteration: "+ i);
			currentVertices = pageRank(gf,i,0.15).orderBy(col("pagerank").desc()).limit(10);
			currentVertices.show();
			if(currentVertices.select("id").toJavaRDD().collect().equals(previousVertices.select("id").toJavaRDD().collect())) {
				System.out.println("Optimum iterations for PageRank : "+i);
				currentVertices.show();
				break;
			}else{
				previousVertices = currentVertices;
			}
		}
		//tune resetProbability
		int maxIterations = 11; //optimum iterations
		double damping = 0.70;
		for(int i =0; i < 6; i++){
			List<Row> distribution = pageRank(gf,maxIterations,1-damping).select("pagerank").toJavaRDD().collect();
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter("dist_" + String.format("%.2f", damping) + ".txt"));
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
			damping += 0.05;
		}
	}
	public static Dataset<Row> pageRank(GraphFrame gf, int iterations, double resetProb){
		Dataset<Row> result = gf.pageRank()
								.maxIter(iterations)
								.resetProbability(resetProb) //damping factor = 1-resetProbability
								.run()
								.vertices();
		return result;
	}


}
