package com.sakame.service;

import com.sakame.model.dto.FinishTaskRequest;
import com.sakame.model.dto.FinishTaskResponse;
import com.sakame.model.dto.GetTaskRequest;
import com.sakame.model.dto.GetTaskResponse;

/**
 * coordinator 服务
 * @author sakame
 * @version 1.0
 */
public interface CoordinatorService {

    /**
     * 发起获取任务请求
     * @param getTaskRequest
     * @return
     */
    public GetTaskResponse getTask(GetTaskRequest getTaskRequest);

    /**
     * 发起完成任务请求
     * @param finishTaskRequest
     * @return
     */
    public FinishTaskResponse finishTask(FinishTaskRequest finishTaskRequest);


}
