package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
*类名和方法不能修改
 */
public class Schedule {

    /**
     * 节点和任务质检的对应关系
     */
    private HashMap<Integer,HashMap<Integer,Integer>> nodeAndTaskMap=new HashMap<>();
    /**
     * 任务编号
     */
    //private HashMap<Integer,Integer> taskList=new HashMap<>();
    /**
     * 挂起任务
     */
    private HashMap<Integer,Integer> waitingList=new HashMap<>();

    public int init() {

        nodeAndTaskMap=new HashMap<>();
        waitingList=new HashMap<>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {

        if (nodeId<=0)
        {
            return ReturnCodeKeys.E004;
        }
        if (nodeAndTaskMap.containsKey(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        nodeAndTaskMap.put(nodeId,new HashMap<Integer,Integer>());
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        //校验参数
        if (nodeId<=0)
        {
            return ReturnCodeKeys.E004;
        }
        if (!nodeAndTaskMap.containsKey(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        //获取注销节点上的任务
        HashMap<Integer,Integer> removeTasks=nodeAndTaskMap.get(nodeId);
        nodeAndTaskMap.remove(nodeId);
        //将注销节点上的任务移至挂起队列
        waitingList.putAll(removeTasks);
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {

        if (taskId<=0)
        {
            return ReturnCodeKeys.E009;
        }
        if(waitingList.containsKey(taskId))
        {
            return ReturnCodeKeys.E010;
        }

        waitingList.put(taskId,consumption);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {

        if (taskId<=0)
        {
            return ReturnCodeKeys.E009;
        }
        if (!waitingList.containsKey(taskId)&&!existInRunningTask(taskId))
        {
            return ReturnCodeKeys.E012;
        }
        waitingList.remove(taskId);

        return ReturnCodeKeys.E011;
    }

    private boolean existInRunningTask(int taskId)
    {
        boolean flag=false;
        for (Map.Entry<Integer, HashMap<Integer, Integer>> entry:nodeAndTaskMap.entrySet())
        {
            if (entry.getValue().containsKey(taskId))
            {
                flag=true;
                break;
            }
        }
        return flag;
    }


    public int scheduleTask(int threshold) {

        if (threshold<=0)
        {
            return ReturnCodeKeys.E002;
        }

        //获取当前任务消耗

        return ReturnCodeKeys.E013;
    }


    private void nodeDiff(int threshold)
    {
        List<Integer> list=new ArrayList<>();
        for (Map.Entry<Integer,HashMap<Integer,Integer>> entry:nodeAndTaskMap.entrySet())
        {
            int sum=0;
            for (Map.Entry<Integer,Integer> task:entry.getValue().entrySet())
            {
                sum+=task.getValue();
            }
            list.add(sum);
        }
        /*for (int i=0;i<list.size();i++)
        {
            for (int j=i+1;j<)
        }*/
    }

    private void judge()
    {
        int consum=0;
        int taskNum=0;
        for (Map.Entry<Integer,Integer> entry:waitingList.entrySet())
        {
            taskNum++;
            consum+=entry.getValue();
        }
        int avg=consum/taskNum;
        HashMap<Integer,Integer> task=waitingList;
        for (Map.Entry<Integer,HashMap<Integer,Integer>> entry:nodeAndTaskMap.entrySet())
        {
            for (int i=1;i<=avg;i++)
            {
                task.remove(i);

            }

        }

    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if (tasks.isEmpty())
        {
            return ReturnCodeKeys.E016;
        }

        return ReturnCodeKeys.E015;
    }

}
