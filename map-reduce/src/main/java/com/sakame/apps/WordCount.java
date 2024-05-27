package com.sakame.apps;

import com.sakame.model.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 计算单词出现次数
 *
 * @author sakame
 * @version 1.0
 */
public class WordCount {
    /**
     * map function
     *
     * @param fileName
     * @param contents
     * @return
     */
    public List<KeyValue> map(String fileName, String contents) {
        List<KeyValue> list = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\b\\w+\\b");
        Matcher matcher = pattern.matcher(contents);
        while (matcher.find()) {
            String word = matcher.group();
            list.add(new KeyValue(word, "1"));
        }

        return list;
    }

    /**
     * reduce function
     *
     * @param key
     * @param values
     * @return
     */
    public String reduce(String key, List<String> values) {
        int result = 0;
        for (String value : values) {
            result += Integer.parseInt(value);
        }
        return String.valueOf(result);
    }
}
