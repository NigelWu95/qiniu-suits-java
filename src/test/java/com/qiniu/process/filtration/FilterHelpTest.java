package com.qiniu.process.filtration;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FilterHelpTest {

    @Test
    public void testLoadCheckJson() {
        try {
            FilterHelp.loadCheckJson();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}