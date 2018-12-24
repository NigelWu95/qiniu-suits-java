package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class FileInputParams extends CommonParams {

    private String filePath;
    private String parseType;
    private String separator;
    private String keyIndex;
    private String hashIndex;
    private String fsizeIndex;
    private String putTimeIndex;
    private String mimeTypeIndex;
    private String endUserIndex;
    private String typeIndex;
    private String statusIndex;
    private String md5Index;
    private String fopsIndex;
    private String persistentIdIndex;
    private String targetKeyIndex;
    private String urlIndex;

    public FileInputParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        this.filePath = entryParam.getParamValue("file-path");
        this.parseType = entryParam.getParamValue("parse-type");
        try { this.separator = entryParam.getParamValue("separator"); } catch (Exception e) {}
        try { this.keyIndex = entryParam.getParamValue("key-index"); } catch (Exception e) {}
        try { this.hashIndex = entryParam.getParamValue("hash-index"); } catch (Exception e) {}
        try { this.fsizeIndex = entryParam.getParamValue("fsize-index"); } catch (Exception e) {}
        try { this.putTimeIndex = entryParam.getParamValue("putTime-index"); } catch (Exception e) {}
        try { this.mimeTypeIndex = entryParam.getParamValue("mimeType-index"); } catch (Exception e) {}
        try { this.endUserIndex = entryParam.getParamValue("endUser-index"); } catch (Exception e) {}
        try { this.typeIndex = entryParam.getParamValue("type-index"); } catch (Exception e) {}
        try { this.statusIndex = entryParam.getParamValue("status-index"); } catch (Exception e) {}
        try { this.md5Index = entryParam.getParamValue("md5-index"); } catch (Exception e) {}
        try { this.fopsIndex = entryParam.getParamValue("fops-index"); } catch (Exception e) {}
        try { this.persistentIdIndex = entryParam.getParamValue("persistentId-index"); } catch (Exception e) {}
        try { this.targetKeyIndex = entryParam.getParamValue("newKey-index"); } catch (Exception e) {}
        try { this.urlIndex = entryParam.getParamValue("url-index"); } catch (Exception e) {}
    }

    public String getFilePath() throws IOException {
        if (filePath == null || "".equals(filePath)) throw new IOException("please set the file path.");
        else if (filePath.startsWith("/")) throw new IOException("the file path only support relative path.");
        return filePath;
    }

    public String getParseType() throws IOException {
        if (parseType == null || "".equals(parseType)) {
            throw new IOException("no incorrect parse type, please set it as \"json\" or \"table\".");
        } else {
            return parseType;
        }
    }

    public String getSeparator() {
        if (separator == null || "".equals(separator)) {
            return "\t";
        } else {
            return separator;
        }
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            return false;
        }
    }

    public String getKeyIndex() throws IOException {
        if (keyIndex == null || "".equals(keyIndex)) {
            if ("json".equals(parseType)) {
                return "key";
            } else {
                return "0";
            }
        } else if (keyIndex.matches("\\d")) {
            return keyIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("not incorrectly set key index, it should be a number.");
            }
            return keyIndex;
        }
    }

    public String getHashIndex() throws IOException {
        if (hashIndex == null || "".equals(hashIndex)) {
            if ("json".equals(parseType)) {
                return "hash";
            } else {
                return "1";
            }
        } else if (hashIndex.matches("\\d")) {
            return hashIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect hash index, it should be a number.");
            }
            return hashIndex;
        }
    }

    public String getFsizeIndex() throws IOException {
        if (fsizeIndex == null || "".equals(fsizeIndex)) {
            if ("json".equals(parseType)) {
                return "fsize";
            } else {
                return "2";
            }
        } else if (fsizeIndex.matches("\\d")) {
            return fsizeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect fsize index, it should be a number.");
            }
            return fsizeIndex;
        }
    }

    public String getPutTimeIndex() throws IOException {
        if (putTimeIndex == null || "".equals(putTimeIndex)) {
            if ("json".equals(parseType)) {
                return "putTime";
            } else {
                return "2";
            }
        } else if (putTimeIndex.matches("\\d")) {
            return putTimeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect putTime index, it should be a number.");
            }
            return putTimeIndex;
        }
    }

    public String getMimeTypeIndex() throws IOException {
        if (mimeTypeIndex == null || "".equals(mimeTypeIndex)) {
            if ("json".equals(parseType)) {
                return "mimeType";
            } else {
                return "4";
            }
        } else if (mimeTypeIndex.matches("\\d")) {
            return mimeTypeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect mimeType index, it should be a number.");
            }
            return mimeTypeIndex;
        }
    }

    public String getEndUserIndex() throws IOException {
        if (endUserIndex == null || "".equals(endUserIndex)) {
            if ("json".equals(parseType)) {
                return "endUser";
            } else {
                return "5";
            }
        } else if (endUserIndex.matches("\\d")) {
            return endUserIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect endUser index, it should be a number.");
            }
            return endUserIndex;
        }
    }

    public String getTypeIndex() throws IOException {
        if (typeIndex == null || "".equals(typeIndex)) {
            if ("json".equals(parseType)) {
                return "type";
            } else {
                return "6";
            }
        } else if (typeIndex.matches("\\d")) {
            return typeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect type index, it should be a number.");
            }
            return typeIndex;
        }
    }

    public String getStatusIndex() throws IOException {
        if (statusIndex == null || "".equals(statusIndex)) {
            if ("json".equals(parseType)) {
                return "status";
            } else {
                return "7";
            }
        } else if (statusIndex.matches("\\d")) {
            return statusIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect status index, it should be a number.");
            }
            return statusIndex;
        }
    }

    public String getMd5Index() throws IOException {
        if (md5Index == null || "".equals(md5Index)) {
            if ("json".equals(parseType)) {
                return "md5";
            } else {
                return "8";
            }
        } else if (md5Index.matches("\\d")) {
            return md5Index;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect md5 index, it should be a number.");
            }
            return md5Index;
        }
    }

    public String getFopsIndex() throws IOException {
        if (fopsIndex == null || "".equals(fopsIndex)) {
            if ("json".equals(parseType)) {
                return "fops";
            } else {
                return "1";
            }
        } else if (fopsIndex.matches("\\d")) {
            return fopsIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect fops index, it should be a number.");
            }
            return fopsIndex;
        }
    }

    public String getPersistentIdIndex() throws IOException {
        if (persistentIdIndex == null || "".equals(persistentIdIndex)) {
            if ("json".equals(parseType)) {
                return "persistentId";
            } else {
                return "0";
            }
        } else if (persistentIdIndex.matches("\\d")) {
            return persistentIdIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect persistentId index, it should be a number.");
            }
            return persistentIdIndex;
        }
    }

    public String getTargetKeyIndex() throws IOException {
        if (targetKeyIndex == null || "".equals(targetKeyIndex)) {
            if ("json".equals(parseType)) {
                throw new IOException("no incorrect json key index for rename's newKey.");
            } else {
                return "1";
            }
        } else if (targetKeyIndex.matches("\\d")) {
            return targetKeyIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect newKey index, it should be a number.");
            }
            return targetKeyIndex;
        }
    }

    public String getUrlIndex() throws IOException {
        if (urlIndex == null || "".equals(urlIndex)) {
            if ("json".equals(parseType)) {
                return "url";
            } else {
                return "0";
            }
        } else if (urlIndex.matches("\\d")) {
            return urlIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new IOException("no incorrect url index, it should be a number.");
            }
            return urlIndex;
        }
    }
}
