package com.qiniu.service.oss;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.model.ListResult;
import com.qiniu.model.ListV2Line;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.*;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class ListFiles {

    private Auth auth;
    private Configuration configuration;
    private BucketManager bucketManager;
    private String bucket;
    private int version;
    private String resultFormat;
    private String resultFileDir;
    private int retryCount;
    private List<String> prefixList;

    public ListFiles(Auth auth, Configuration configuration, String bucket, int version, String resultFormat,
                     String resultFileDir, List<String> prefixList, int retryCount) {
        this.auth = auth;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(auth, configuration);
        this.bucket = bucket;
        this.version = version;
        this.prefixList = prefixList;
        this.retryCount = retryCount;
        this.resultFormat = StringUtils.isNullOrEmpty(resultFormat) ? "json" : resultFormat;
        this.resultFileDir = StringUtils.isNullOrEmpty(resultFileDir) ? "../result" : resultFileDir;
    }

    /*
    v2 的 list 接口，通过 IO 流的方式返回文本信息，v1 是单次请求的结果一次性返回。
     */
    public Response list(String prefix, String delimiter, String marker, int limit) throws QiniuException {

        Response response = null;
        try {
            response = version == 2 ?
                    bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                    bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    System.out.println("listV" + version + " " + bucket + ":" + prefix + ":" + marker + ":" + limit
                            + ":" + delimiter + " " + e1.error() + ", last " + retryCount + " times retry...");
                    response = version == 2 ?
                            bucketManager.listV2(bucket, prefix, marker, limit, delimiter) :
                            bucketManager.listV1(bucket, prefix, marker, limit, delimiter);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }
        return response;
    }

    public ListResult getListResult(String prefix, String delimiter, String marker, int limit) throws QiniuException {

        ListResult listResult = new ListResult();
        Response response = list(prefix, delimiter, marker, limit);
        if (response != null) {
            if (version == 1) {
                FileListing fileListing = response.jsonToObject(FileListing.class);
                if (fileListing != null) {
                    FileInfo[] items = fileListing.items;
                    listResult.nextMarker = fileListing.marker;
                    if (items.length > 0) listResult.fileInfoList = Arrays.asList(items);
                }
            } else if (version == 2) {
                InputStream inputStream = new BufferedInputStream(response.bodyStream());
                Reader reader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(reader);
                List<ListV2Line> listV2LineList = bufferedReader.lines().parallel()
                        .filter(line -> !StringUtils.isNullOrEmpty(line))
                        .map(line -> new ListV2Line().fromLine(line))
                        .collect(Collectors.toList());
                listResult.fileInfoList = listV2LineList.parallelStream()
                        .map(listV2Line -> listV2Line.fileInfo)
                        .collect(Collectors.toList());
                Optional<ListV2Line> lastListV2Line = listV2LineList.parallelStream()
                        .max(ListV2Line::compareTo);
                lastListV2Line.ifPresent(listV2Line -> listResult.nextMarker = listV2Line.marker);
            }
            response.close();
        }
        return listResult;
    }

    private List<ListResult> getListResultByPrefix(List<String> prefixList, int unitLen) {

        return prefixList.parallelStream()
                .map(prefix -> {
                    try {
                        ListResult listResult = getListResult(prefix, null, null, unitLen);
                        listResult.commonPrefix = prefix;
                        return listResult;
                    } catch (QiniuException e) {
                        throw new RuntimeException(prefix + "\t" + e.error(), e);
                    }
                })
                .filter(ListResult::isValid)
                .collect(Collectors.toList());
    }

    public void processFile() throws QiniuException {
        Iterator iterator = new ArrayList<>().iterator();
    }
}