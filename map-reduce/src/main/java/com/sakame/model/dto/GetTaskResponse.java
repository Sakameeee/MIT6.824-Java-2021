package com.sakame.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 获取任务响应
 * @author sakame
 * @version 1.0
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GetTaskResponse implements Serializable {
    /**
     * 任务 id
     */
    private int taskId;

    /**
     * 文件列表
     */
    private List<String> fileNames;

    /**
     * 任务类型：map，reduce
     */
    private int type;

    // single?
    private int bucketNumber;
    /**
     * map 任务为切片个数
     */
    private int nReduce;
}
