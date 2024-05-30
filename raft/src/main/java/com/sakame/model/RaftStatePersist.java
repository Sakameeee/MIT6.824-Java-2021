package com.sakame.model;

import lombok.Data;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class RaftStatePersist {

    private int currentTerm;

    private int votedFor;

    private Entry[] logs;

}
