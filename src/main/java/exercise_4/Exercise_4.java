package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.nio.Buffer;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {
		java.util.List<Row> vertices_list = new ArrayList<Row>();
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
		java.util.List<Row> edges_list = new ArrayList<Row>();
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



	}

}
