package com.qiniu.custom;

import com.qiniu.persistence.FileMap;
import com.qiniu.service.convert.MapToString;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.service.interfaces.ITypeConvert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CCheckMime implements ILineProcess<Map<String, String>>, Cloneable {

    final private String processName;
    private String resultTag;
    private int resultIndex;
    private String resultPath;
    private FileMap fileMap;
    private ITypeConvert<Map<String, String>, String> typeConverter;
    final private List<String> extMimeList = new ArrayList<String>(){{
        add("ico:image");
        add("ief:image");
        add("jp2:image");
        add("jpe:image");
        add("jpeg:image");
        add("webp:image");
        add("gif:image");
        add("png:image");
        add("jpg:image");
        add("heic:image");
        add("mac:image");
        add("pbm:image");
        add("pct:image");
        add("pgm:image");
        add("pic:image");
        add("pict:image");
        add("pnm:image");
        add("pnt:image");
        add("pntg:image");
        add("ppm:image");
        add("qti:image");
        add("qtif:image");
        add("ras:image");
        add("rgb:image");
        add("svg:image");
        add("tif:image");
        add("tiff:image");
        add("wbmp:image");
        add("xbm:image");
        add("xpm:image");
        add("xwd:image");
        add("bmp:image");
        add("cgm:image");
        add("djv:image");
        add("djvu:image");

        add("aif:audio");
        add("aifc:audio");
        add("aiff:audio");
        add("au:audio");
        add("m3u:audio");
        add("m4p:audio");
        add("kar:audio");
        add("mid:audio");
        add("midi:audio");
        add("mp2:audio");
        add("mp3:audio");
        add("aac:audio");
        add("amr:audio");
        add("ape:audio");
        add("flac:audio");
        add("m4a:audio");
        add("mpga:audio");
        add("ra:audio");
        add("ram:audio");
        add("wav:audio");
        add("snd:audio");

        add("m4u:video");
        add("m4v:video");
        add("3gp:video");
        add("avi:video");
        add("dif:video");
        add("dv:video");
        add("flv:video");
        add("mkv:video");
        add("ts:video");
        add("mpe:video");
        add("mpeg:video");
        add("mpg:video");
        add("mxu:video");
        add("ogv:video");
        add("wmv:video");
        add("webm:video");
        add("qt:video");
        add("mov:video");
        add("movie:video");
        add("wma:video");
        add("m4a:video");
        add("mp4:video");
        add("mp3:video");
        add("aac:video");
    }};
    final private List<String> extMimeTypeList = new ArrayList<String>(){{
        add("apk:application/vnd.android.package-archive");
        add("ai:application/postscript");
        add("asc:text/plain");
        add("atom:application/atom+xml");
        add("bcpio:application/x-bcpio");
        add("bin:application/octet-stream");
        add("cdf:application/x-netcdf");
        add("class:application/octet-stream");
        add("cpio:application/x-cpio");
        add("cpt:application/mac-compactpro");
        add("csh:application/x-csh");
        add("css:text/css");
        add("dcr:application/x-director");
        add("dir:application/x-director");
        add("dll:application/octet-stream");
        add("dmg:application/octet-stream");
        add("dms:application/octet-stream");
        add("doc:application/msword");
        add("dtd:application/xml-dtd");
        add("dvi:application/x-dvi");
        add("dxr:application/x-director");
        add("eps:application/postscript");
        add("etx:text/x-setext");
        add("exe:application/octet-stream");
        add("ez:application/andrew-inset");
        add("gram:application/srgs");
        add("grxml:application/srgs+xml");
        add("gtar:application/x-gtar");
        add("gz:application/x-gzip");
        add("hdf:application/x-hdf");
        add("hqx:application/mac-binhex40");
        add("htm:text/html");
        add("html:text/html");
        add("ice:x-conference/x-cooltalk");
        add("ics:text/calendar");
        add("ifb:text/calendar");
        add("iges:model/iges");
        add("igs:model/iges");
        add("jnlp:application/x-java-jnlp-file");
        add("js:application/x-javascript");
        add("latex:application/x-latex");
        add("lha:application/octet-stream");
        add("lzh:application/octet-stream");
        add("m3u8:application/x-mpegurl");
        add("m3u8:application/vnd.apple.mpegurl");
        add("man:application/x-troff-man");
        add("mathml:application/mathml+xml");
        add("me:application/x-troff-me");
        add("mesh:model/mesh");
        add("mif:application/vnd.mif");
        add("ms:application/x-troff-ms");
        add("msh:model/mesh");
        add("nc:application/x-netcdf");
        add("oda:application/oda");
        add("ogg:application/ogg");
        add("pdb:chemical/x-pdb");
        add("pdf:application/pdf");
        add("pgn:application/x-chess-pgn");
        add("ppt:application/vnd.ms-powerpoint");
        add("ps:application/postscript");
        add("rdf:application/rdf+xml");
        add("rm:application/vnd.rn-realmedia");
        add("roff:application/x-troff");
        add("rtf:text/rtf");
        add("rtx:text/richtext");
        add("sgm:text/sgml");
        add("sgml:text/sgml");
        add("sh:application/x-sh");
        add("shar:application/x-shar");
        add("silo:model/mesh");
        add("sit:application/x-stuffit");
        add("skd:application/x-koan");
        add("skm:application/x-koan");
        add("skp:application/x-koan");
        add("skt:application/x-koan");
        add("smi:application/smil");
        add("smil:application/smil");
        add("so:application/octet-stream");
        add("spl:application/x-futuresplash");
        add("src:application/x-wais-source");
        add("sv4cpio:application/x-sv4cpio");
        add("sv4crc:application/x-sv4crc");
        add("swf:application/x-shockwave-flash");
        add("t:application/x-troff");
        add("tar:application/x-tar");
        add("tcl:application/x-tcl");
        add("tex:application/x-tex");
        add("texi:application/x-texinfo");
        add("texinfo:application/x-texinfo");
        add("tr:application/x-troff");
        add("tsv:text/tab-separated-values");
        add("txt:text/plain");
        add("ustar:application/x-ustar");
        add("vcd:application/x-cdlink");
        add("vrml:model/vrml");
        add("vxml:application/voicexml+xml");
        add("wbxml:application/vnd.wap.wbxml");
        add("wml:text/vnd.wap.wml");
        add("wmlc:application/vnd.wap.wmlc");
        add("wmls:text/vnd.wap.wmlscript");
        add("wmlsc:application/vnd.wap.wmlscriptc");
        add("wrl:model/vrml");
        add("xht:application/xhtml+xml");
        add("xhtml:application/xhtml+xml");
        add("xls:application/vnd.ms-excel");
        add("xml:application/xml");
        add("xsl:application/xml");
        add("xslt:application/xslt+xml");
        add("xul:application/vnd.mozilla.xul+xml");
        add("xyz:chemical/x-xyz");
        add("zip:application/zip");
    }};

    public CCheckMime(String resultPath, String resultFormat, String resultSeparator, List<String> rmFields,
                      int resultIndex) throws IOException {
        this.processName = "checkmime";
        this.resultPath = resultPath;
        this.resultTag = "";
        this.resultIndex = resultIndex;
        this.fileMap = new FileMap(resultPath, processName, String.valueOf(resultIndex));
        this.fileMap.initDefaultWriters();
        this.typeConverter = new MapToString(resultFormat, resultSeparator, rmFields);
    }

    public CCheckMime(String resultPath, String resultFormat, String resultSeparator, List<String> rmFields)
            throws IOException {
        this(resultPath, resultFormat, resultSeparator, rmFields, 0);
    }

    public String getProcessName() {
        return this.processName;
    }

    public void setResultTag(String resultTag) {
        this.resultTag = resultTag == null ? "" : resultTag;
    }

    public CCheckMime clone() throws CloneNotSupportedException {
        CCheckMime cCheckMime = (CCheckMime)super.clone();
        cCheckMime.fileMap = new FileMap(resultPath, processName, resultTag + String.valueOf(++resultIndex));
        try {
            cCheckMime.fileMap.initDefaultWriters();
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return cCheckMime;
    }

    public void processLine(List<Map<String, String>> lineList) throws IOException {
        String key;
        List<Map<String, String>> filteredList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            key = line.get("key");
            if (key != null && key.contains(".")) {
                String finalKeyMimePair = key.substring(key.lastIndexOf(".") + 1) + ":" + line.get("mimeType");
                if (extMimeList.parallelStream().anyMatch(extMime ->
                        finalKeyMimePair.split("/")[0].equalsIgnoreCase(extMime))) {
                    break;
                }
                if (extMimeTypeList.parallelStream().noneMatch(extMime -> finalKeyMimePair.startsWith(extMime) ||
                        finalKeyMimePair.equalsIgnoreCase(extMime))) {
                    filteredList.add(line);
                }
            }
        }
        List<String> writeList = typeConverter.convertToVList(filteredList);
        if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
        if (typeConverter.getErrorList().size() > 0)
            fileMap.writeError(String.join("\n", typeConverter.getErrorList()));
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
