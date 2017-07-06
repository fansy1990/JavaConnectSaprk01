package demo03;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Created by fansy on 2017/7/5.
 */
public class Driver {
    public static void main(String[] args) {
//        <input> <output> <appName> <master>"
//        " <jars> <logEnabled> <logDir>
        String[] arg = new String[]{
            "hdfs://node10:8020/user/root/magic",
                "",
                "wordcount" + System.currentTimeMillis(),
                "spark://node10:7077",
                "C:\\Users\\fansy\\workspace_idea_tmp\\JavaConnectSaprk01\\out\\artifacts\\wordcount\\wordcount.jar",
                "true",
                "hdfs://node10:8020/eventLog"
        };
        FutureTask<Boolean> future = new FutureTask<>(new RunTool(arg));
        new Thread(future).start();
        boolean flag = true;
        while(flag){
            try{
                Thread.sleep(2000);
                System.out.println("Job running ...");
                if(future.isDone()){
                    flag = false;
                    if(future.get().booleanValue()){
                        System.out.println("Job done with success state");
                    }else{
                        System.out.println("Job failed!");
                    }
                }
            }catch (InterruptedException|ExecutionException e){
                e.printStackTrace();
            }


        }
    }
}
