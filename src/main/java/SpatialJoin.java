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

public class SpatialJoin {

    public static class PointMapper
            extends Mapper<Object, Text, Text, Text> {
        //Keeps track of what dataset it came from
        private final static String P = "P";
        private final static int size = 10;
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //Convert the record into a string array
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Parse the record
            int x = Integer.parseInt(record[0]);
            int y = Integer.parseInt(record[1]);

            //Continue if not in spatial window
            int[] spatialWindow = parseSpatialWindow(context.getConfiguration().get("spatialWindow"));
            int windowX1 = spatialWindow[0];
            int windowY1 = spatialWindow[1];
            int windowX2 = spatialWindow[2];
            int windowY2 = spatialWindow[3];

            if (x < windowX1 || x > windowX2 || y < windowY1 || y > windowY2){
                return;
            }

            //Determine section point belongs in
            //For example (16, 402) -> (10, 400)
            String rectangleX = String.valueOf((x / size) * size);
            String rectangleY = String.valueOf((y / size) * size);
            id.set(rectangleX + "," + rectangleY);



            //Make a custom value for the mapper to include where it came from
            String reduceValue = P + "," + x + "," + y;
            context.write(id, new Text(reduceValue));
        }

        private int[] parseSpatialWindow(String spatialWindow){
            int[] coords = new int[4];
            String[] record = spatialWindow.split(",");
            coords[0] = Integer.parseInt(record[0]);
            coords[1] = Integer.parseInt(record[1]);
            coords[2] = Integer.parseInt(record[2]);
            coords[3] = Integer.parseInt(record[3]);
            return coords;
        }
    }

    public static class RectangleMapper
            extends Mapper<Object, Text, Text, Text> {
        //Keeps track of what dataset it came from
        private final static String R = "R";
        private final static int size = 10;
        private Text id = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Convert the record into a string array
            String recordString = value.toString();
            String[] record = recordString.split(",");

            //Extract the information
            int bottomLeftX = Integer.parseInt(record[0]);
            int bottomLeftY = Integer.parseInt(record[1]);
            int height = Integer.parseInt(record[2]);
            int width = Integer.parseInt(record[3]);

            int maxX = bottomLeftX + width;
            int maxY = bottomLeftY + height;

            //Continue if not in spatial window
            int[] spatialWindow = parseSpatialWindow(context.getConfiguration().get("spatialWindow"));
            int windowX1 = spatialWindow[0];
            int windowY1 = spatialWindow[1];
            int windowX2 = spatialWindow[2];
            int windowY2 = spatialWindow[3];

            if (maxX < windowX1 || bottomLeftX > windowX2 || maxY < windowY1 || bottomLeftY > windowY2){
                return;
            }

            //Make a custom value for the mapper to include where it came from
            String reduceValue = R + "," + bottomLeftX + "," +  bottomLeftY + "," + height + "," + width;
            Text reduceText = new Text(reduceValue);
            //Find all the subsections that this rectangle belongs to and put it in each one
            for (int x = bottomLeftX; x < maxX; x += size){
                for (int y = bottomLeftY; y < maxY; y += size){
                    String rectangleX = String.valueOf((x / size) * size);
                    String rectangleY = String.valueOf((y / size) * size);
                    id.set(rectangleX + "," + rectangleY);
                    context.write(id, reduceText);
                }
            }
        }

        private int[] parseSpatialWindow(String spatialWindow){
            int[] coords = new int[4];
            String[] record = spatialWindow.split(",");
            coords[0] = Integer.parseInt(record[0]);
            coords[1] = Integer.parseInt(record[1]);
            coords[2] = Integer.parseInt(record[2]);
            coords[3] = Integer.parseInt(record[3]);
            return coords;
        }
    }

    public static class RectangleReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Variables for the final output
            ArrayList<String> listOfPoints = new ArrayList<String>();
            ArrayList<String> listOfRectangles = new ArrayList<String>();


            //Sort the values to whether they are in points list or rectangles list
            for (Text val : values) {
                String recordString = val.toString();
                String[] record = recordString.split(",");
                String dataset = record[0];
                if (dataset.equals("P")){
                    listOfPoints.add(recordString);
                } else {
                    listOfRectangles.add(recordString);
                }
            }

            //For each rectangle determine if the point is inside of it
            for (String rectangleString : listOfRectangles){
                for (String pointString : listOfPoints) {
                    if (insideRectangle(pointString, rectangleString)){
                        //Create custom value for output
                        String[] rectangle = rectangleString.split(",");
                        String rectanglePart = "(" + rectangle[1] + "," + rectangle[2] + "," + rectangle[3] + "," + rectangle[4] + ")";
                        String[] point = pointString.split(",");
                        String pointPart = "(" + point[1] + "," + point[2] +  ")";

                        String result = rectanglePart + "," + pointPart;
                        context.write(null, new Text(result));
                    }
                }
            }
        }

        private boolean insideRectangle(String pointString, String rectangleString){
            //R + bottomLeftX + bottomLeftY + "," + height + "," + width;
            //P + "," + x + "," + y;
            //Parse the point data
            String[] point = pointString.split(",");
            int x = Integer.parseInt(point[1]);
            int y = Integer.parseInt(point[2]);

            //Parse the rectangle data
            String[] rectangle = rectangleString.split(",");
            int bottomLeftX = Integer.parseInt(rectangle[1]);
            int bottomLeftY = Integer.parseInt(rectangle[2]);
            int height = Integer.parseInt(rectangle[3]);
            int width = Integer.parseInt(rectangle[4]);

            return x > bottomLeftX && x < (bottomLeftX + width) && y > bottomLeftY && y < (bottomLeftY + height);
        }
    }

    public static void main(String[] args) throws Exception {
        // points rectangles outputSpatial 1 1 10000 10000
        String pointsFile = "";
        String rectangleFile = "";
        String outputFile = "";
        String spatialWindow = "";

        if (args.length >=3){
            pointsFile = args[0];
            rectangleFile = args[1];
            outputFile = args[2];
        } else {
            System.err.println("Parameters were incorrectly passed in. Should be:");
            System.err.println("pointsFile rectangleFile outputFile x1 y1 x2 y2");
            System.err.println("The coordinates are optional parameters");
            System.exit(1);
        }

        if (args.length == 7){
            String x1 = args[3];
            String y1 = args[4];
            String x2 = args[5];
            String y2 = args[6];
            spatialWindow = x1 + "," + y1 + "," + x2 + "," + y2;
        } else {
            spatialWindow = "1,1,10000,10000";
        }

        Configuration conf = new Configuration();
        conf.set("spatialWindow", spatialWindow);
        Job job = Job.getInstance(conf, "Spatial Join");
        job.setJarByClass(SpatialJoin.class);
        job.setReducerClass(RectangleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(pointsFile), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(rectangleFile), TextInputFormat.class, RectangleMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
