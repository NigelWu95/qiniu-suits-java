package com.qiniu.process.filtration;

import com.qiniu.convert.MapToString;
import com.qiniu.interfaces.ITypeConvert;
import com.qiniu.persistence.FileSaveMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapProcess extends FilterProcess<Map<String, String>> {

    public MapProcess(BaseFilter<Map<String, String>> baseFilter, SeniorFilter<Map<String, String>> seniorFilter,
                      String savePath, String saveFormat, String saveSeparator, Set<String> rmFields, int saveIndex)
            throws Exception {
        super(baseFilter, seniorFilter, savePath, saveFormat, saveSeparator, rmFields, saveIndex);
    }

    public MapProcess(BaseFilter<Map<String, String>> baseFilter, SeniorFilter<Map<String, String>> seniorFilter,
                      String savePath, String saveFormat, String saveSeparator, Set<String> rmFields) throws Exception {
        this(baseFilter, seniorFilter, savePath, saveFormat, saveSeparator, rmFields, 0);
    }

    public void updateSavePath(String savePath) throws IOException {
        this.savePath = savePath;
        this.fileSaveMapper.closeWriters();
        this.fileSaveMapper = new FileSaveMapper(savePath, processName, String.valueOf(saveIndex));
    }

    @Override
    protected ITypeConvert<Map<String, String>, String> newTypeConverter() throws IOException {
        return new MapToString(saveFormat, saveSeparator, rmFields);
    }
}
