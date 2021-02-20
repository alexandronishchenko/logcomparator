package ru.x5;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    private static File kafkaLog;
    private static List<File> nativeLogs;
    private static Date startTime;
    private static Date endTime;

    private static TreeSet<Date> kafkaTime;
    private static TreeSet<Date> logTime;

    private static HashMap<Date,String> logMessages=new HashMap<>();

    public static void main(String[] args) {
        // write your code here
        String kafkaFile = args[0];
        String logDir = args[1];
        setInterval(args[2],args[3]);
        initFiles(kafkaFile,logDir);
        kafkaTime=new TreeSet<>();
        loadStrings(kafkaTime,"kafka");
        logTime=new TreeSet<>();
        loadStrings(logTime,"log");
        System.out.println("Lost: "+compare()+" percent.");

    }

    private static void setInterval(String start,String end){
        try {
            startTime=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS").parse(start);
            endTime=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS").parse(end);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private static void initFiles(String kafka, String logDir){
        kafkaLog=new File(kafka);
        nativeLogs = new ArrayList<>();
        File dir = new File(logDir);
        File[] files = new File(logDir).listFiles();
        for(File file : files)nativeLogs.add(file);
    }

    private static void loadStrings(TreeSet<Date> tree,String input){
        TreeSet<Date> dates=tree;
        try {
            FileReader curReader = null;
            if (input.equals("kafka")) {
                curReader = new FileReader(kafkaLog);
                    readFile(curReader,tree,"kafka");
            } else if (input.equals("log")) {
                for (File file : nativeLogs) {
                    curReader = new FileReader(file);
                    readFile(curReader,tree,"log");
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static void readFile(FileReader curReader,TreeSet<Date> dates,String input){
        try {
        BufferedReader reader = new BufferedReader(curReader);
        while (reader.ready()){
            String line = reader.readLine();
//            System.out.println(line);
            Date cur = null;
            try {
                String sub= null;
                if(input.equals("kafka")) {
                    if(line.length()>35){
                    sub=line.substring(12, 35);}
//                    System.out.println(sub);
                }else if(input.equals("log")) {
                    try {
                        sub = line.substring(0, 23);
                    }catch (StringIndexOutOfBoundsException ex){
                        //System.out.println(line +" cannot be parsed.");
                    }
                }
                if(sub!=null) {
                    cur = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS").parse(sub);
                    logMessages.put(cur,line);
                    if (cur.after(startTime) && cur.before(endTime)) {
                        dates.add(cur);
                    }
                }
            } catch (ParseException e) {
//e.printStackTrace();
            }
//            if(cur!=null) {
//
//            }
        }
        reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static double compare(){
        double percentage=0;
        int found=0;
        int lost=0;
        for(Date date : logTime){
            if(kafkaTime.contains(date)){
                ++found;
//                System.out.println("Log: "+date);
            }else {
                ++lost;
                System.out.println("Lost was: " + logMessages.get(date));
            }
        }
        System.out.println("found: "+found);
        System.out.println("lost: "+lost);
        System.out.println("total messages at kafka: "+kafkaTime.size()+ " total messages at std log: "+logTime.size());
        percentage=100*lost/logTime.size();
        return percentage;
    }
}
