package com.qiniu.model.parameter;

import com.qiniu.service.interfaces.IEntryParam;

import java.io.IOException;

public class FieldIndexParams extends CommonParams {

    private String keyIndex;
    private String hashIndex;
    private String fsizeIndex;
    private String putTimeIndex;
    private String mimeTypeIndex;
    private String endUserIndex;
    private String typeIndex;
    private String statusIndex;

    public FieldIndexParams(IEntryParam entryParam) throws Exception {
        super(entryParam);
        try { this.keyIndex = entryParam.getParamValue("key-index"); } catch (Exception e) {}
        try { this.hashIndex = entryParam.getParamValue("hash-index"); } catch (Exception e) {}
        try { this.fsizeIndex = entryParam.getParamValue("fsize-index"); } catch (Exception e) {}
        try { this.putTimeIndex = entryParam.getParamValue("putTime-index"); } catch (Exception e) {}
        try { this.mimeTypeIndex = entryParam.getParamValue("mimeType-index"); } catch (Exception e) {}
        try { this.endUserIndex = entryParam.getParamValue("endUser-index"); } catch (Exception e) {}
        try { this.typeIndex = entryParam.getParamValue("type-index"); } catch (Exception e) {}
        try { this.statusIndex = entryParam.getParamValue("status-index"); } catch (Exception e) {}
    }

    public String getKeyIndex() throws IOException {
        if (keyIndex == null || "".equals(keyIndex)) {
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
            if ("json".equals(getParseType())) {
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
}
