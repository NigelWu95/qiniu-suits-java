package com.qiniu.service.oss;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.qiniu.common.FileReaderAndWriterMap;
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
import java.util.stream.Stream;

public class ListFiles {

    private Auth auth;
    private Configuration configuration;
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
    public Response list(BucketManager bucketManager, String prefix, String delimiter, String marker, int limit)
            throws QiniuException {

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

    private void writeResult(List<FileInfo> fileInfoList, FileReaderAndWriterMap fileMap, int writeType) {

        if (fileInfoList == null || fileInfoList.size() == 0) return;
        if (fileMap != null) {
            Stream<FileInfo> fileInfoStream = fileInfoList.parallelStream().filter(Objects::nonNull);
            List<String> list = resultFormat.equals("json") ?
                    fileInfoStream.map(JsonConvertUtils::toJsonWithoutUrlEscape).collect(Collectors.toList()) :
                    fileInfoStream.map(LineUtils::toSeparatedItemLine).collect(Collectors.toList());
            if (writeType == 1) fileMap.writeSuccess(String.join("\n", list));
            if (writeType == 2) fileMap.writeOther(String.join("\n", list));
        }
    }

    public ListV2Line getItemByList2Line(String line) {

        ListV2Line listV2Line = new ListV2Line();
        if (!StringUtils.isNullOrEmpty(line)) {
            JsonObject json = new JsonObject();
            // to test the exceptional line.
            try {
                json = JsonConvertUtils.toJsonObject(line);
            } catch (JsonParseException e) {
                System.out.println(line);
                e.printStackTrace();
            }
            JsonElement item = json.get("item");
            JsonElement marker = json.get("marker");
            if (item != null && !(item instanceof JsonNull)) {
                listV2Line.fileInfo = JsonConvertUtils.fromJson(item, FileInfo.class);
            }
            if (marker != null && !(marker instanceof JsonNull)) {
                listV2Line.marker = marker.getAsString();
            }
        }
        return listV2Line;
    }

    public ListResult getListResult(Response response) throws QiniuException {

        ListResult listResult = new ListResult();
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
                        .map(this::getItemByList2Line)
                        .collect(Collectors.toList());
                listResult.fileInfoList = listV2LineList.parallelStream()
                        .map(listV2Line -> listV2Line.fileInfo)
                        .collect(Collectors.toList());
                Optional<ListV2Line> lastListV2Line = listV2LineList.parallelStream()
                        .max(ListV2Line::compareTo);
                lastListV2Line.ifPresent(listV2Line -> listResult.nextMarker = listV2Line.marker);
            }
        }

        return listResult;
    }

    private List<ListResult> preListByPrefix(BucketManager bucketManager, List<String> prefixList, int unitLen,
                                             String resultPrefix) throws IOException {
        FileReaderAndWriterMap fileMap = new FileReaderAndWriterMap();
        fileMap.initWriter(resultFileDir, resultPrefix, "pre");
        List<ListResult> listResultList = prefixList.parallelStream()
                .map(prefix -> {
                    Response response = null;
                    ListResult listResult = new ListResult();
                    try {
                        response = list(bucketManager, prefix, null, null, unitLen);
                        listResult = getListResult(response);
                        listResult.commonPrefix = prefix;
                    } catch (QiniuException e) {
                        fileMap.writeErrorOrNull(prefix + "\t" + e.error());
                    } finally {
                        if (response != null) response.close();
                    }
                    return listResult;
                })
                .filter(ListResult::isValid)
                .collect(Collectors.toList());
        fileMap.closeWriter();
        return listResultList;
    }

    public List<ListResult> preList(int unitLen, int level, String customPrefix, List<String> antiPrefix,
                                    String resultPrefix) throws IOException {
        List<String> validPrefixList = prefixList.parallelStream()
                .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                .map(prefix -> StringUtils.isNullOrEmpty(customPrefix) ? prefix : customPrefix + prefix)
                .collect(Collectors.toList());
        List<ListResult> listResultList = new ArrayList<>();
        BucketManager bucketManager = new BucketManager(auth, configuration);
        if (level == 1) {
            listResultList = preListByPrefix(bucketManager, validPrefixList, unitLen, resultPrefix);
        } else if (level == 2) {
            listResultList = preListByPrefix(bucketManager, validPrefixList, 1, resultPrefix);
            List<String> level2PrefixList = listResultList.parallelStream()
                    .map(singlePrefixListResult -> prefixList.parallelStream()
                            .filter(originPrefix -> !antiPrefix.contains(originPrefix))
                            .map(originPrefix -> singlePrefixListResult.commonPrefix + originPrefix)
                            .collect(Collectors.toList()))
                    .reduce((list1, list2) -> {
                        list1.addAll(list2);
                        return list1;
                    }).get();
            listResultList = preListByPrefix(bucketManager, level2PrefixList, unitLen, resultPrefix);
        }

        return listResultList;
    }

    public void processFile(List<FileInfo> fileInfoList, int retryCount) throws QiniuException {

    }
}