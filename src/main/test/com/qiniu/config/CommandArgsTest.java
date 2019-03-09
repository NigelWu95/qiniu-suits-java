package com.qiniu.config;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class CommandArgsTest {

    private CommandArgs commandArgs;

    @Before
    public void init() throws IOException {
        String[] args = new String[]{"list", "-a=\"\"", "-ak=\"1\"", "-sk=1", "-bucket=1", "-multi=1", "-max-threads=1"};
        commandArgs = new CommandArgs(args);
    }

    @Test
    public void testGetValue() throws IOException {
        System.out.println(commandArgs.getValue("ak"));
        System.out.println(commandArgs.getValue("ab", "ab"));
    }

    @Test
    public void testGetParams() {
        System.out.println(Arrays.toString(commandArgs.getParams()));
    }
}
