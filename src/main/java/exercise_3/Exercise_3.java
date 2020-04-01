package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import exercise_2.Exercise_2;
import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.*;


public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,ArrayList<Long>,ArrayList<Long>,ArrayList<Long>> implements Serializable {
        @Override
        public ArrayList<Long> apply(Long vertexID, ArrayList<Long> vertexValue, ArrayList<Long> message) {

            //Superstep 0
            if (message.get(0) == Long.MAX_VALUE) {
                return vertexValue;
            }

            //Superstep>0
            else {
                // check the cost of current vertex and incoming message
                if (Math.abs(message.get(0)) >= Math.abs(vertexValue.get(0))) {
                    // add this vertex to the path and return
                    vertexValue.add(vertexID);
                    return vertexValue;
                }
                else {
                    // add this vertex to the path and return
                    message.add(vertexID);
                    return message;
                }
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<ArrayList<Long>, Integer>, scala.collection.Iterator<Tuple2<Object, ArrayList<Long>>>> implements Serializable {
        @Override
        public scala.collection.Iterator<Tuple2<Object, ArrayList<Long>>> apply(EdgeTriplet<ArrayList<Long>, Integer> triplet) {

            Tuple2<Object, ArrayList<Long>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, ArrayList<Long>> dstVertex = triplet.toTuple()._2();
            Long attr = (long)triplet.attr; // message value

        if (Math.abs(dstVertex._2.get(0)) <= Math.abs(sourceVertex._2.get(0)) + attr) {
            //  if source vertex value + message value is greater than dst vertex value => Do nothing
            return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, ArrayList<Long>>>().iterator()).asScala();
        } else {
            // otherwise propagate source vertex value => Send the message with message value = ArrayList of the source vertex
            ArrayList<Long> result = sourceVertex._2; // temporary variable
            result.set(0, Math.abs(result.get(0))+ attr); // update the cost by changing the first element int the ArrayList
            return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,ArrayList<Long>>(triplet.dstId(),result)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<ArrayList<Long>, ArrayList<Long>, ArrayList<Long>> implements Serializable {
        @Override
        public ArrayList<Long> apply(ArrayList<Long> o, ArrayList<Long> o2) {
            // Choose the minimum value among incoming messages
            if(Math.abs(o.get(0))<=Math.abs(o2.get(0)))
                return o;
            else
                return o2;
        }
    }
    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        /*
        * Each vertex value is an ArrayList: arr[]
        * The first value in the ArrayList represent the cost from A to the vertex itself
        * The path is represented by a chain of vertexID starting from the second value in the Arraylist above
        * For example:
        * arr[0]: cost of the shortest path
        * arr[1], arr[2], arr[3], ... : vertexIDs in the shortest path
        * */
        
        List<Tuple2<Object, ArrayList<Long>>> vertices = Lists.newArrayList(
                new Tuple2<Object, ArrayList<Long>>(1l, new ArrayList<Long>(Arrays.asList(0l, 1l))),
                new Tuple2<Object, ArrayList<Long>>(2l, new ArrayList<Long>(Arrays.asList(Long.MAX_VALUE))),
                new Tuple2<Object, ArrayList<Long>>(3l, new ArrayList<Long>(Arrays.asList(Long.MAX_VALUE))),
                new Tuple2<Object, ArrayList<Long>>(4l, new ArrayList<Long>(Arrays.asList(Long.MAX_VALUE))),
                new Tuple2<Object, ArrayList<Long>>(5l, new ArrayList<Long>(Arrays.asList(Long.MAX_VALUE))),
                new Tuple2<Object, ArrayList<Long>>(6l, new ArrayList<Long>(Arrays.asList(Long.MAX_VALUE)))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );
        JavaRDD<Tuple2<Object, ArrayList<Long>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<ArrayList<Long>, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), new ArrayList<Long>(Arrays.asList(0l)), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(ArrayList.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(ArrayList.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new ArrayList<Long>(Arrays.asList(Long.MAX_VALUE)),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(ArrayList.class))
                .vertices()
                .toJavaRDD()
                .sortBy(f -> ((Tuple2<Object, ArrayList<Long>>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object, ArrayList<Long>> vertex = (Tuple2<Object, ArrayList<Long>>) v;

                    // Get label for result path
                    String path = "[";
                    for (int i = 1; i < vertex._2.size(); i++) {
                        path += labels.get(vertex._2.get(i));
                        if(i==vertex._2.size()-1)
                            path += "]";
                        else
                            path += ",";
                    }

                    System.out.println("Minimum path to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+
                            path + " with cost " + vertex._2.get(0));
                });
    }
}
