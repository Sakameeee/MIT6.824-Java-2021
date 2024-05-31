package com.sakame.model.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
public class InstallSnapshotResponse implements Serializable {
    private int term;
}
