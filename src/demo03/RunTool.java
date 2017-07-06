package demo03;


import java.util.concurrent.Callable;

/**
 * 线程任务
 * Created by fansy on 2017/7/5.
 */
public class RunTool implements Callable {

    private String input;
    private String output;
    private String appName;
    private String master;
    private String jars;
    private String logEnabled;
    private String logDir;

    public RunTool(){}
    public RunTool(String[] args){
        this.input = args[0];
        this.output = args[1];
        this.appName = args[2];
        this.master = args[3];
        this.jars = args[4];
        this.logEnabled = args[5];
        this.logDir = args[6];
    }

    @Override
    public Boolean call() throws Exception {
        return WordCount.run(new String[]{input,output,appName,master,jars,logEnabled,logDir});
    }
}
