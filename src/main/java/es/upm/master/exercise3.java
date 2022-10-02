package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class exercise3 {

    public static void main(String[] args) {

        // set up the execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path and user input
        DataStream<String> text = env.readTextFile(params.get("input"));

        final int segUser = Integer.parseInt(params.get("segment"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Set type of time window
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // EXERCISE 3.1:

        // obtain tuples
        // input: <Time, VID, Spd, Xway, Lane, Dir, Seg, Pos>
        // output: <VID, xway, avgSpeed>

        // Transform input and transform in a tuple6
        SingleOutputStreamOperator<Tuple6<String, String, Integer, Integer, Integer, Long>> mapStream = text.map(new MapFunction<String, Tuple6<String, String, Integer, Integer, Integer, Long>>() {
            public Tuple6<String, String, Integer, Integer, Integer, Long> map(String in) throws Exception {
                String[] fieldArray = in.split(",");

                String vid = fieldArray[1];
                Integer speed = Integer.parseInt(fieldArray[2]);
                String xway = fieldArray[3];
                Integer seg = Integer.parseInt(fieldArray[6]);
                long time = Integer.parseInt(fieldArray[0]);

                Tuple6<String, String, Integer, Integer, Integer, Long> out = new Tuple6<String, String, Integer, Integer, Integer, Long>(vid, xway, speed, seg, 1, time);
                return out;

            }
        });


        // keep only cars in segment == segmentUser
        SingleOutputStreamOperator<Tuple6<String, String, Integer, Integer, Integer, Long>> filteredStream = mapStream.filter(new FilterFunction<Tuple6<String, String, Integer, Integer, Integer, Long>>() {
            public boolean filter(Tuple6<String, String, Integer, Integer, Integer, Long> in) throws Exception {
                return in.f3.equals(segUser);
            }
        });

        // Stablish time as timestamp
        KeyedStream streamWindow = filteredStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple6<String, String, Integer, Integer, Integer, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple6<String, String, Integer, Integer, Integer, Long> element) {
                        return element.f5 * 1000; // timestamp from millis to seconds
                    }
                }).keyBy(0); // group by VID


        // calculate the sum of speed of each car in segUser:

        SingleOutputStreamOperator<Tuple6<String, String, Integer, Integer, Integer, Long>> sumSpeedStream = streamWindow.
                window(TumblingEventTimeWindows.of(Time.seconds(3600)))
                .reduce(new ReduceFunction<Tuple6<String, String, Integer, Integer, Integer, Long>>() {
            public Tuple6<String, String, Integer, Integer, Integer, Long> reduce(Tuple6<String, String, Integer, Integer, Integer, Long> o, Tuple6<String, String, Integer, Integer, Integer, Long> t1) throws Exception {

                // keep the shortest time
                long time = o.f5;
                if(o.f5 > t1.f5) {
                    time = t1.f5;
                }
                // calculate the sum of speed for each car
                Integer speed = o.f2 + t1.f2;

                // calculate the number of apparitions for each car in segUser
                Integer nCarEvents = o.f4 + t1.f4;
                Tuple6<String, String, Integer, Integer, Integer, Long> out = new Tuple6<String, String, Integer, Integer, Integer, Long>(o.f0, o.f1, speed, o.f3, nCarEvents, time);
                return out;
            }
        });

        // Caculate avg speed for each car
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Long>> avgSpeed = sumSpeedStream.map(new MapFunction<Tuple6<String, String, Integer, Integer, Integer, Long>, Tuple4<String, String, Integer, Long>>() {
            public Tuple4<String, String, Integer, Long> map(Tuple6<String, String, Integer, Integer, Integer, Long> in) throws Exception {
                Integer avgSpeed = in.f2 / in.f4;

                Tuple4<String, String, Integer, Long> out = new Tuple4<String, String, Integer, Long>(in.f0, in.f1, avgSpeed, in.f5);
                return out;
            }
        });

        // Prepare tuples for output format of exercise 3.1
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resultEx1 = avgSpeed.map(new MapFunction<Tuple4<String, String, Integer, Long>, Tuple3<String, String, Integer>>() {
            public Tuple3<String, String, Integer> map(Tuple4<String, String, Integer, Long> in) throws Exception {

                Tuple3<String, String, Integer> result = new Tuple3<String, String, Integer>(in.f0, in.f1, in.f2);
                return result;
            }
        });

        // write solution exercise 3.1
        String file1 = params.get("output1");
        resultEx1.writeAsCsv(file1, FileSystem.WriteMode.OVERWRITE);


        // EXERCISE 3.2:

        // input: <VID, xway, avgSpeed, time>
        // output: <VID, xway, avgSpeedOfHighestAvgSpeed>


        // Create a non-keyed window for calculate the highest avg speed
        // time window of 3600s = 1h.
        // output <VID, xway, highestSpeedOfHighestAvgSpeed, time>
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Long>> highestSpeed = avgSpeed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3600)))
                .reduce(new ReduceFunction<Tuple4<String, String, Integer, Long>>() {
                    public Tuple4<String, String, Integer, Long> reduce(Tuple4<String, String, Integer, Long> o, Tuple4<String, String, Integer, Long> t1) {

                        // found higher speed than o
                        if(t1.f2 > o.f2) { // current car have the highest speed
                            return new Tuple4<String, String, Integer, Long>(t1.f0, t1.f1, t1.f2, t1.f3);
                        }
                        // not found higher speed than o
                        else{
                            return new Tuple4<String, String, Integer, Long>(o.f0, o.f1, o.f2, o.f3);
                        }

                    }
                });


        // Output the required format for exercise 3.2
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resultEx2 = highestSpeed.map(new MapFunction<Tuple4<String, String, Integer, Long>, Tuple3<String, String, Integer>>() {
            public Tuple3<String, String, Integer> map(Tuple4<String, String, Integer, Long> in) throws Exception {
                return new Tuple3<String, String, Integer>(in.f0, in.f1, in.f2);
            }
        });

        // write solution exercise 3.2
        String file2 = params.get("output2");
        resultEx2.writeAsCsv(file2, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("exercise3");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
