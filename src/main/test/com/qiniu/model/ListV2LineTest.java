package com.qiniu.model;

import com.qiniu.storage.model.FileInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class ListV2LineTest {

    @Test
    public void testCompareTo() {
        List<ListV2Line> listV2LineList = new ArrayList<>();
        ListV2Line listV2Line = new ListV2Line();
        FileInfo fileInfo = new FileInfo();
        fileInfo.key = "test";
        fileInfo.type = 0;
        listV2Line.fileInfo = fileInfo;
        listV2LineList.add(new ListV2Line());
        listV2LineList.add(listV2Line);
        Optional<ListV2Line> lastListV2Line = listV2LineList.parallelStream().max(ListV2Line::compareTo);
        Assert.assertNull(lastListV2Line.get().fileInfo);
    }
}