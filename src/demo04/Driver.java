package demo04;


import org.apache.spark.SparkContext;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStatusTracker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Created by fansy on 2017/7/5.
 */
public class Driver {
    public static void main(String[] args) throws InterruptedException {
        String master = "spark://node10:7077";
        String appName = "wordcount" + System.currentTimeMillis();
        String[] jars = "C:\\Users\\fansy\\workspace_idea_tmp\\JavaConnectSaprk01\\out\\artifacts\\wordcount\\wordcount.jar".split(",");
        String logEnabled = "true";
        String logDir = "hdfs://node10:8020/eventLog";

        String[] arg = new String[]{
                "hdfs://node10:8020/user/root/magic",
                ""
        };

        // 1.获取SC
        SparkContext sc = Utils.getSc(master, appName, jars, logEnabled, logDir);

        // 2. 提交任务 线程
        FutureTask<Boolean> future = new FutureTask<>(new WordCount(sc, arg));
        new Thread(future).start();

        // 3. 监控
        String appId = sc.applicationId();
        System.out.println("AppId:"+appId);
        SparkStatusTracker sparkStatusTracker = null;
        int[] jobIds ;
        SparkJobInfo jobInfo;
        while (!sc.isStopped()) {// 如果sc没有stop，则往下监控
            Thread.sleep(2000);
            // 获取所有Job
            sparkStatusTracker = sc.statusTracker();
            jobIds = sparkStatusTracker.getJobIdsForGroup(null);
            for(int jobId :jobIds){
                jobInfo = sparkStatusTracker.getJobInfo(jobId).getOrElse(null);
                if(jobInfo == null){
                    System.out.println("JobId:"+jobId+",相关信息获取不到!");
                }else{
                    System.out.println("JobId:" + jobId + ",任务状态：" + jobInfo.status().name());
                }
            }
        }

        // 4. 检查线程任务是否返回true
        boolean flag = true;
        while(flag){
            try{
                Thread.sleep(200);
                System.out.println("Job closing ...");
                if(future.isDone()){
                    flag = false;
                    if(future.get().booleanValue()){
                        System.out.println("Job "+appId+" done with success state");
                    }else{
                        System.out.println("Job "+appId+" failed!");
                    }
                }
            }catch (InterruptedException|ExecutionException e){
                e.printStackTrace();
            }

        }

    }
}
