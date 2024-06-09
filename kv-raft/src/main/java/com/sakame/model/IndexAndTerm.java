package com.sakame.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * @author sakame
 * @version 1.0
 */
@Data
@AllArgsConstructor
public class IndexAndTerm implements Serializable {

    private int index;

    private int term;

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        IndexAndTerm indexAndTerm = (IndexAndTerm) obj;
        return indexAndTerm.index == this.index && indexAndTerm.term == this.term;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(index);
        result = 31 * result + Integer.hashCode(term);
        return result;
    }

}
