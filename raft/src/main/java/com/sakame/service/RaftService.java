package com.sakame.service;

import com.sakame.model.dto.*;

/**
 * 用于 rpc 服务调用的接口
 *
 * @author sakame
 * @version 1.0
 */
public interface RaftService {

    /**
     * 处理接收到的投票请求
     *
     * @param request
     * @return
     */
    RequestVoteResponse requestVote(RequestVoteRequest request);

    /**
     * 处理来自 leader 的心跳请求
     */
    void requestHeartbeat();

    /**
     * 处理来自 leader 的追加日志请求
     *
     * @param request
     * @return
     */
    AppendEntriesResponse requestAppendEntries(AppendEntriesRequest request);

    InstallSnapshotResponse requestInstallSnapshot(InstallSnapshotRequest request);

}
