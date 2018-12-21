package com.qiniu.config;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CommandArgsTest {

    private CommandArgs commandArgs;

    @Before
    public void init() throws IOException {
        String[] args = new String[]{"list", "-ak=1", "-sk=1", "-bucket=1", "-multi=1", "-max-threads=1"};
        commandArgs = new CommandArgs(args);
    }

    @Test
    public void getParamValue() throws IOException {
        commandArgs.getParamValue("ak");
    }
}
