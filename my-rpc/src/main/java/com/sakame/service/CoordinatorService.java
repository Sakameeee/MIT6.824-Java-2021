package com.sakame.service;

import com.sakame.model.mr.FinishTaskArgs;
import com.sakame.model.mr.FinishTaskReply;
import com.sakame.model.mr.GetTaskArgs;
import com.sakame.model.mr.GetTaskReply;

/**
 * coordinator 服务
 * @author sakame
 * @version 1.0
 */
public interface CoordinatorService {

    /**
     * 发起获取任务请求
     * @param getTaskArgs
     * @return
     */
    public GetTaskReply getTask(GetTaskArgs getTaskArgs);

    /**
     * 发起完成任务请求
     * @param finishTaskArgs
     * @return
     */
    public FinishTaskReply finishTask(FinishTaskArgs finishTaskArgs);


}
