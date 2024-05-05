package com.sakame.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author sakame
 * @version 1.0
 */
@Data
@AllArgsConstructor
public class KeyValue {
    private String key;

    private String value;
}
