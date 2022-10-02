package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
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

import java.util.ArrayList;
import java.util.List;

//Input data: vehiclesData.csv (all ints)
//(Timestamp (seconds) 0, Vehicle ID 1, Speed (0-100) 2, Expressway ID (0-9) 3, Lane (0-4) 4
// Direction (0-east, 1-west) 5, Segment (0-99) 6, Position from westernmost (0-527999) 7

/*For each xway and every X seconds spots the number of vehicles
that have reported an average speed higher than Y
between a couple of segments z and w in eastbound direction, along with the list of VIDs of the cars

time (1st vehicle), xway, numberOfVehicles, VIDs ( [1- 353 - ... - N] ).
*/

public class exercise2 {
    public static void main(String[] args) {

        // set up the execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // read the text file from given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        final int Xseconds = Integer.parseInt(params.get("time"));
        final int Seg0 = Integer.parseInt(params.get("startSegment"));
        final int Seg1 = Integer.parseInt(params.get("endSegment"));
        final int Speed = Integer.parseInt(params.get("speed"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //obtain tuples
        SingleOutputStreamOperator<Tuple7<Long, String, Double, String, String, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple7<Long, String, Double, String, String, Integer, Integer>>() {
                    public Tuple7<Long, String, Double, String, String, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");

                        long time = Integer.parseInt(fieldArray[0]);
                        String vid = fieldArray[1];
                        Double speed = Double.parseDouble(fieldArray[2]);
                        String xway = fieldArray[3];
                        String dir = fieldArray[5];
                        Integer seg = Integer.parseInt(fieldArray[6]);

                        Tuple7<Long, String, Double, String, String, Integer, Integer> out = new Tuple7<Long, String, Double, String, String, Integer, Integer>(time, vid, speed, xway, dir, seg, 1);
                        return out;
                    }
                });


        // keep only cars going to east and between seg0 and seg1 (I guess both included)
        SingleOutputStreamOperator<Tuple7<Long, String, Double, String, String, Integer, Integer>> filteredStream = mapStream.
                filter(new FilterFunction<Tuple7<Long, String, Double, String, String, Integer, Integer>>() {
                    public boolean filter(Tuple7<Long, String, Double, String, String, Integer, Integer> in) throws Exception {
                        return in.f4.equals("0") && Seg0 <= in.f5 && in.f5 <= Seg1;
                    }
                });

        //Stablish time as timestamp and key by VID
        KeyedStream keyedStream = filteredStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple7<Long, String, Double, String, String, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple7<Long, String, Double, String, String, Integer, Integer> element) {
                        return element.f0 * 1000; // timestamp from millis to seconds
                    }
                }

        ).keyBy(1); //key by the VID

        //create window of Xseconds
        //reduce by key VID, obtain sum speed of each car on each segment
        SingleOutputStreamOperator<Tuple7<Long, String, Double, String, String, Integer, Integer>> sumSpeedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(Xseconds)))
                .reduce(new ReduceFunction<Tuple7<Long, String, Double, String, String, Integer, Integer>>() {
                    public Tuple7<Long, String, Double, String, String, Integer, Integer> reduce(Tuple7<Long, String, Double, String, String, Integer, Integer> o, Tuple7<Long, String, Double, String, String, Integer, Integer> t) throws Exception {
                        //keep the shortest time
                        long time = o.f0;
                        if (o.f0 > t.f0) {
                            time = t.f0;
                        }
                        //obtain the sum of speeds
                        Double speed = o.f2 + t.f2;
                        //obtain the number of times this car appears                                                                      //(time, vid, speed, xway, dir, seg,1)
                        int n = o.f6 + t.f6;
                        Tuple7<Long, String, Double, String, String, Integer, Integer> p = new Tuple7<Long, String, Double, String, String, Integer, Integer>(time, o.f1, speed, o.f3, o.f4, o.f5, n);
                        return p;
                    }
                });

        //(time, vid, speed, xway, dir, seg,n) to:
        //(time,list(vid),avg(speed),xway)
        // keep those with speed higher than Y
        // keyby the exway
        KeyedStream avgSpeedStream = sumSpeedStream.
                map(new MapFunction<Tuple7<Long, String, Double, String, String, Integer, Integer>, Tuple4<Long, List<String>, Double, String>>() {
                    public Tuple4<Long, List<String>, Double, String> map(Tuple7<Long, String, Double, String, String, Integer, Integer> in) throws Exception {
                        List<String> vid = new ArrayList<String>();
                        vid.add(in.f1);
                        Double speed = in.f2 / in.f6;

                        Tuple4<Long, List<String>, Double, String> out = new Tuple4<Long, List<String>, Double, String>(in.f0, vid, speed, in.f3);
                        return out;
                    }
                }).filter(new FilterFunction<Tuple4<Long, List<String>, Double, String>>() {
            public boolean filter(Tuple4<Long, List<String>, Double, String> in) throws Exception {
                return in.f2 > Speed;
            }
        }).keyBy(3);

        //collapse window keeping min(timeStamp), creating list
        //(time,list(vid),avg(speed),xway)
        SingleOutputStreamOperator<Tuple4<Long, List<String>, Double, String>> mergedVIDs = avgSpeedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(Xseconds)))
                .reduce(new ReduceFunction<Tuple4<Long, List<String>, Double, String>>() {
                    public Tuple4<Long, List<String>, Double, String> reduce(Tuple4<Long, List<String>, Double, String> o, Tuple4<Long, List<String>, Double, String> t1) throws Exception {
                        long time = o.f0;
                        if(o.f0> t1.f0){
                            time = t1.f0;
                        }
                        List<String> newList = new ArrayList<String>();
                        newList.addAll(o.f1);
                        newList.addAll(t1.f1);
                        Tuple4<Long, List<String>, Double, String> out = new Tuple4<Long, List<String>, Double, String>(time, newList, o.f2, o.f3);
                        return out;
                    }
                });

        //map keeping timeStamp, xway, size(list), string list with format [ vid1 - ... - vidSize ]
        SingleOutputStreamOperator<Tuple4<Long, String,Integer,String>> result = mergedVIDs
        .map(new MapFunction<Tuple4<Long, List<String>, Double, String>, Tuple4<Long, String,Integer,String>>() {
            public Tuple4<Long, String,Integer,String> map(Tuple4<Long, List<String>, Double, String> in) throws Exception {
                int size = in.f1.size();
                String lista ="[ ";
                for (int i=0 ; i<(size-1); i++){
                    lista += in.f1.get(i) + " - ";
                }
                lista += in.f1.get(size-1) + " ]";
                Tuple4<Long, String,Integer,String> out = new Tuple4<Long, String,Integer,String>(in.f0, in.f3, size, lista);
                return out;
            }
        });

        // write the result
        String file = params.get("output");
        result.writeAsCsv(file, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("exercise2");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}