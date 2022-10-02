package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
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


//Input data: vehiclesData.csv (all ints)
//(Timestamp (seconds), Vehicle ID, Speed (0-100), Expressway ID (0-9), Lane (0-4)
// Direction (0-east, 1-west), Segment (0-99), Position from westernmost (0-527999)

//Output calculate the number of cars that use each exit lane every hour
//Timestamp of the first car (1->0), Expressway ID(4->3), exitLane(5->4), numberOfVehicles(count)

public class exercise1 {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //obtain tuples
        SingleOutputStreamOperator<Tuple4<Long,String,String,Integer>> mapStream = text.
                map(new MapFunction<String, Tuple4<Long,String,String,Integer>>() {
                    public Tuple4<Long,String,String,Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");

                        long time = Integer.parseInt(fieldArray[0]);
                        String exway = fieldArray[3];
                        String exlane = fieldArray[4];

                        Tuple4<Long,String,String,Integer> out = new Tuple4<Long,String,String,Integer>(time, exway, exlane,1);
                        return out;
                    }
                });

        //keep only the exits (lane 4)
        SingleOutputStreamOperator<Tuple4<Long,String,String,Integer>> filteredStream = mapStream.
                filter(new FilterFunction<Tuple4<Long,String,String,Integer>>() {
                    public boolean filter(Tuple4<Long,String,String,Integer> in) throws Exception {
                        return in.f2.equals("4");
                    }
                });

        // stablish timestamp for the window and key by expressway
        KeyedStream keyedStream = filteredStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple4<Long,String,String,Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Long,String,String,Integer> element) {
                        return element.f0*1000; // timestamp * 1000 to transform to seconds (Flink thinks they are millis)
                    }
                }

        ).keyBy(1); //key by the expressway

        //stablish window of size 3600 seconds = 1 hour
        // reduce to get a count for each hour, keeping the lowest possible timestamp
        SingleOutputStreamOperator<Tuple4<Long,String,String,Integer>> result = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3600))).reduce(
                new ReduceFunction<Tuple4<Long,String,String,Integer>>() {

                    public Tuple4<Long, String, String, Integer> reduce(Tuple4<Long, String, String, Integer> o, Tuple4<Long, String, String, Integer> t) throws Exception {
                        long time = o.f0;
                        if (o.f0 > t.f0) {
                            time = t.f0;
                        }
                        int n = o.f3 + t.f3;
                        Tuple4<Long, String, String, Integer> p = new Tuple4<Long, String, String, Integer>(time, o.f1, o.f2, n);
                        return p;
                    }
                }
        );
        // write the result
        String file = params.get("output");
        result.writeAsCsv(file, FileSystem.WriteMode.OVERWRITE);

        // execute program
        env.execute("exercise1");
    }

}