import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class OutlierDetection {

    public static class OutlierMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text id = new Text();
        private int maxSize = 10000;

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Get the chunks that we will divide the points into
            int sizeOfChunk = 100;


            //Convert the record into a string array
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Parse the record
            int x = Integer.parseInt(record[0]);
            int y = Integer.parseInt(record[1]);

            //Get the radius
            int radius = Integer.parseInt(context.getConfiguration().get("r"));

            //Get all the chunks that relate to the point
            ArrayList<int[]> chunks = getChunks(sizeOfChunk, x, y, radius);
            //The value is the point
            String reduceValue = x + "," + y;

            //If the circle around the point intersects a chunk then insert it
            for (int[] chunk : chunks){
                //The id is the chunk
                id.set(chunk[0] + "," + chunk[1] + "," + chunk[2] + "," +chunk[3]);
                context.write(id, new Text(reduceValue));
            }
        }

        //Get chunks within the circle
        private ArrayList<int[]> getChunks(int size, int xc, int yc, int radius){
            ArrayList<int[]> listOfChunks = new ArrayList<int[]>();

            int bottomLeftX = xc - radius;
            int bottomLeftY = yc - radius;

            int bottomLeftXChunk = (bottomLeftX / size) * size;
            int bottomLeftYChunk = (bottomLeftY / size) * size;

            int topRightX = xc + radius;
            int topRightY = yc + radius;

            for (int x = bottomLeftXChunk; x < topRightX; x += size){
                for (int y = bottomLeftYChunk; y < topRightY; y += size){
                    //Ensures we don't add chunks that aren't in the space
                    if (x < 0 || y < 0 || x >= maxSize || y >= maxSize){
                        continue;
                    }
                    int[] arr = new int[]{x,y,x + size, y + size};
                    listOfChunks.add(arr);
                }
            }
            return listOfChunks;
        }
    }

    public static class OutlierReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Variables for the final output
            ArrayList<int[]> points = new ArrayList<int[]>();

            //Store all of the points
            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");
                int[] arr = new int[]{Integer.parseInt(record[0]), Integer.parseInt(record[1])};
                points.add(arr);
            }

            //Get the max radius
            double radius = Double.parseDouble(context.getConfiguration().get("r"));

            //Get k
            int k = Integer.parseInt(context.getConfiguration().get("k"));

            for (int[] potentialPoint : points){
                if(!insideRectangle(potentialPoint, key.toString())){
                    continue;
                }

                int numPointsWithinR = 0;
                for (int[] point : points){
                    if (radius >= euclideanDistance(potentialPoint[0], potentialPoint[1], point[0], point[1])){
                        numPointsWithinR++;
                    }
                }

                //Check if the number of points is less than k
                //Subtract one because the point is counted twice
                if (numPointsWithinR - 1 < k){
                    String result = potentialPoint[0] + "," + potentialPoint[1];
                    context.write(null, new Text(result));
                }
            }
        }

        private double euclideanDistance(double x1, double y1, double x2, double y2) {
            double deltaX = x2 - x1;
            double deltaY = y2 - y1;
            return Math.sqrt(deltaX * deltaX + deltaY * deltaY);
        }

        private boolean insideRectangle(int[] point, String rectangleString){

            //Parse the point data
            int x = point[0];
            int y = point[1];

            //Parse the rectangle data
            String[] rectangle = rectangleString.split(",");
            int bottomLeftX = Integer.parseInt(rectangle[0]);
            int bottomLeftY = Integer.parseInt(rectangle[1]);
            int topRightX = Integer.parseInt(rectangle[2]);
            int topRightY = Integer.parseInt(rectangle[3]);

            return x > bottomLeftX && x < topRightX && y > bottomLeftY && y < topRightY;
        }
    }

    public static void main(String[] args) throws Exception {
        // points outputOutlier 5 10
        String pointsFile = "";
        String outputFile = "";
        String r = "";
        String k = "";

        if (args.length == 4){
            pointsFile = args[0];
            outputFile = args[1];
            r = args[2];
            k = args[3];
        } else {
            System.err.println("Parameters were incorrectly passed in. Should be:");
            System.err.println("pointsFile outputFile r k");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("r", r);
        conf.set("k", k);
        Job job = Job.getInstance(conf, "Outlier Detection");
        job.setJarByClass(OutlierDetection.class);
        job.setReducerClass(OutlierReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(pointsFile), TextInputFormat.class, OutlierMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
