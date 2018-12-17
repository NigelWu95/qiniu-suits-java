package com.qiniu.model.parameter;

import com.qiniu.common.QiniuException;

public class FileInputParams extends CommonParams {

    private String filePath;
    private String unitLen;
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

    public FileInputParams(String[] args) throws Exception {
        super(args);
        this.filePath = getParamFromArgs("file-path");
        try { this.unitLen = getParamFromArgs("unit-len"); } catch (Exception e) {}
        try { this.parseType = getParamFromArgs("parse-type"); } catch (Exception e) {}
        try { this.separator = getParamFromArgs("separator"); } catch (Exception e) {}
        try { this.keyIndex = getParamFromArgs("key-index"); } catch (Exception e) {}
        try { this.hashIndex = getParamFromArgs("hash-index"); } catch (Exception e) {}
        try { this.fsizeIndex = getParamFromArgs("fsize-index"); } catch (Exception e) {}
        try { this.putTimeIndex = getParamFromArgs("putTime-index"); } catch (Exception e) {}
        try { this.mimeTypeIndex = getParamFromArgs("mimeType-index"); } catch (Exception e) {}
        try { this.endUserIndex = getParamFromArgs("endUser-index"); } catch (Exception e) {}
        try { this.typeIndex = getParamFromArgs("type-index"); } catch (Exception e) {}
        try { this.statusIndex = getParamFromArgs("status-index"); } catch (Exception e) {}
        try { this.md5Index = getParamFromArgs("md5-index"); } catch (Exception e) {}
        try { this.fopsIndex = getParamFromArgs("fops-index"); } catch (Exception e) {}
        try { this.persistentIdIndex = getParamFromArgs("persistentId-index"); } catch (Exception e) {}
    }

    public FileInputParams(String configFileName) throws Exception {
        super(configFileName);
        this.filePath = getParamFromConfig("file-path");
        try { this.unitLen = getParamFromConfig("unit-len"); } catch (Exception e) {}
        try { this.parseType = getParamFromConfig("parser-type"); } catch (Exception e) {}
        try { this.separator = getParamFromConfig("separator"); } catch (Exception e) {}
        try { this.keyIndex = getParamFromConfig("key-index"); } catch (Exception e) {}
        try { this.hashIndex = getParamFromConfig("hash-index"); } catch (Exception e) {}
        try { this.fsizeIndex = getParamFromConfig("fsize-index"); } catch (Exception e) {}
        try { this.putTimeIndex = getParamFromConfig("putTime-index"); } catch (Exception e) {}
        try { this.mimeTypeIndex = getParamFromConfig("mimeType-index"); } catch (Exception e) {}
        try { this.endUserIndex = getParamFromConfig("endUser-index"); } catch (Exception e) {}
        try { this.typeIndex = getParamFromConfig("type-index"); } catch (Exception e) {}
        try { this.statusIndex = getParamFromConfig("status-index"); } catch (Exception e) {}
        try { this.md5Index = getParamFromConfig("md5-index"); } catch (Exception e) {}
        try { this.fopsIndex = getParamFromConfig("fops-index"); } catch (Exception e) {}
        try { this.persistentIdIndex = getParamFromConfig("persistentId-index"); } catch (Exception e) {}
    }

    public String getParseType() {
        if (parseType == null || "".equals(parseType)) {
            System.out.println("no incorrect parse type, it will use \"json\" as default.");
            return "json";
        } else {
            return parseType;
        }
    }

    public String getSeparator() {
        if (separator == null || "".equals(separator)) {
            System.out.println("no incorrect separator, it will use \"\t\" as default.");
            return "\t";
        } else {
            return separator;
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public int getUnitLen() {
        if (unitLen.matches("\\d+")) {
            return Integer.valueOf(unitLen);
        } else {
            System.out.println("no incorrect unit-len, it will use 1000 as default.");
            return 1000;
        }
    }

    public Boolean getSaveTotal() {
        if (saveTotal.matches("(true|false)")) {
            return Boolean.valueOf(saveTotal);
        } else {
            System.out.println("not incorrectly set result save total option, it will use \"false\" as default.");
            return false;
        }
    }

    public String getKeyIndex() throws QiniuException {
        if (keyIndex == null || "".equals(keyIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect key index, it will use \"key\" as default");
                return "key";
            } else {
                System.out.println("no incorrect key index, it will use 0 as default");
                return "0";
            }
        } else if (keyIndex.matches("\\d")) {
            return keyIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "not incorrectly set key index, it should be a number.");
            }
            return keyIndex;
        }
    }

    public String getHashIndex() throws QiniuException {
        if (hashIndex == null || "".equals(hashIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect hash index, it will use \"hash\" as default");
                return "hash";
            } else {
                System.out.println("no incorrect hash index, it will use 1 as default");
                return "1";
            }
        } else if (hashIndex.matches("\\d")) {
            return hashIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect hash index, it should be a number.");
            }
            return hashIndex;
        }
    }

    public String getFsizeIndex() throws QiniuException {
        if (fsizeIndex == null || "".equals(fsizeIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect fsize index, it will use \"fsize\" as default");
                return "fsize";
            } else {
                System.out.println("no incorrect fsize index, it will use 2 as default");
                return "2";
            }
        } else if (fsizeIndex.matches("\\d")) {
            return fsizeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect fsize index, it should be a number.");
            }
            return fsizeIndex;
        }
    }

    public String getPutTimeIndex() throws QiniuException {
        if (putTimeIndex == null || "".equals(putTimeIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect putTime index, it will use \"putTime\" as default");
                return "putTime";
            } else {
                System.out.println("no incorrect putTime index, it will use 3 as default");
                return "2";
            }
        } else if (putTimeIndex.matches("\\d")) {
            return putTimeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect putTime index, it should be a number.");
            }
            return putTimeIndex;
        }
    }

    public String getMimeTypeIndex() throws QiniuException {
        if (mimeTypeIndex == null || "".equals(mimeTypeIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect mimeType index, it will use \"mimeType\" as default");
                return "mimeType";
            } else {
                System.out.println("no incorrect mimeType index, it will use 4 as default");
                return "4";
            }
        } else if (mimeTypeIndex.matches("\\d")) {
            return mimeTypeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect mimeType index, it should be a number.");
            }
            return mimeTypeIndex;
        }
    }

    public String getEndUserIndex() throws QiniuException {
        if (endUserIndex == null || "".equals(endUserIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect endUser index, it will use \"endUser\" as default");
                return "endUser";
            } else {
                System.out.println("no incorrect endUser index, it will use 5 as default");
                return "5";
            }
        } else if (endUserIndex.matches("\\d")) {
            return endUserIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect endUser index, it should be a number.");
            }
            return endUserIndex;
        }
    }

    public String getTypeIndex() throws QiniuException {
        if (typeIndex == null || "".equals(typeIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect type index, it will use \"type\" as default");
                return "type";
            } else {
                System.out.println("no incorrect type index, it will use 6 as default");
                return "6";
            }
        } else if (typeIndex.matches("\\d")) {
            return typeIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect type index, it should be a number.");
            }
            return typeIndex;
        }
    }

    public String getStatusIndex() throws QiniuException {
        if (statusIndex == null || "".equals(statusIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect status index, it will use \"status\" as default");
                return "status";
            } else {
                System.out.println("no incorrect status index, it will use 7 as default");
                return "7";
            }
        } else if (statusIndex.matches("\\d")) {
            return statusIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect status index, it should be a number.");
            }
            return statusIndex;
        }
    }

    public String getMd5Index() throws QiniuException {
        if (md5Index == null || "".equals(md5Index)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect md5 index, it will use \"md5\" as default");
                return "md5";
            } else {
                System.out.println("no incorrect md5 index, it will use 8 as default");
                return "8";
            }
        } else if (md5Index.matches("\\d")) {
            return md5Index;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect md5 index, it should be a number.");
            }
            return md5Index;
        }
    }

    public String getFopsIndex() throws QiniuException {
        if (fopsIndex == null || "".equals(fopsIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect fops index, it will use \"fops\" as default");
                return "fops";
            } else {
                System.out.println("no incorrect fops index, it will use 1 as default");
                return "1";
            }
        } else if (fopsIndex.matches("\\d")) {
            return fopsIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect fops index, it should be a number.");
            }
            return fopsIndex;
        }
    }

    public String getPersistentIdIndex() throws QiniuException {
        if (persistentIdIndex == null || "".equals(persistentIdIndex)) {
            if ("json".equals(parseType)) {
                System.out.println("no incorrect persistentId index, it will use \"persistentId\" as default");
                return "persistentId";
            } else {
                System.out.println("no incorrect persistentId index, it will use 0 as default");
                return "0";
            }
        } else if (persistentIdIndex.matches("\\d")) {
            return persistentIdIndex;
        } else {
            if (!"json".equals(getParseType())) {
                throw new QiniuException(null, "no incorrect persistentId index, it should be a number.");
            }
            return persistentIdIndex;
        }
    }
}
