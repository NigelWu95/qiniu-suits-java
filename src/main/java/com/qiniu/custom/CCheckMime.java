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
    private List<String> extMimeList = new ArrayList<String>(){{
        add("apk:application/vnd.android.package-archive");
        add("3gp:video/3gpp");
        add("ai:application/postscript");
        add("aif:audio/x-aiff");
        add("aifc:audio/x-aiff");
        add("aiff:audio/x-aiff");
        add("asc:text/plain");
        add("atom:application/atom+xml");
        add("au:audio/basic");
        add("avi:video/x-msvideo");
        add("bcpio:application/x-bcpio");
        add("bin:application/octet-stream");
        add("bmp:image/bmp");
        add("cdf:application/x-netcdf");
        add("cgm:image/cgm");
        add("class:application/octet-stream");
        add("cpio:application/x-cpio");
        add("cpt:application/mac-compactpro");
        add("csh:application/x-csh");
        add("css:text/css");
        add("dcr:application/x-director");
        add("dif:video/x-dv");
        add("dir:application/x-director");
        add("djv:image/vnd.djvu");
        add("djvu:image/vnd.djvu");
        add("dll:application/octet-stream");
        add("dmg:application/octet-stream");
        add("dms:application/octet-stream");
        add("doc:application/msword");
        add("dtd:application/xml-dtd");
        add("dv:video/x-dv");
        add("dvi:application/x-dvi");
        add("dxr:application/x-director");
        add("eps:application/postscript");
        add("etx:text/x-setext");
        add("exe:application/octet-stream");
        add("ez:application/andrew-inset");
        add("flv:video/x-flv");
        add("gif:image/gif");
        add("gram:application/srgs");
        add("grxml:application/srgs+xml");
        add("gtar:application/x-gtar");
        add("gz:application/x-gzip");
        add("hdf:application/x-hdf");
        add("hqx:application/mac-binhex40");
        add("htm:text/html");
        add("html:text/html");
        add("ice:x-conference/x-cooltalk");
        add("ico:image/x-icon");
        add("ics:text/calendar");
        add("ief:image/ief");
        add("ifb:text/calendar");
        add("iges:model/iges");
        add("igs:model/iges");
        add("jnlp:application/x-java-jnlp-file");
        add("jp2:image/jp2");
        add("jpe:image/jpeg");
        add("jpeg:image/jpeg");
        add("jpg:image/jpeg");
        add("js:application/x-javascript");
        add("kar:audio/midi");
        add("latex:application/x-latex");
        add("lha:application/octet-stream");
        add("lzh:application/octet-stream");
        add("m3u:audio/x-mpegurl");
        add("m4a:audio/mp4a-latm");
        add("m4p:audio/mp4a-latm");
        add("m4u:video/vnd.mpegurl");
        add("m4v:video/x-m4v");
        add("mac:image/x-macpaint");
        add("man:application/x-troff-man");
        add("mathml:application/mathml+xml");
        add("me:application/x-troff-me");
        add("mesh:model/mesh");
        add("mid:audio/midi");
        add("midi:audio/midi");
        add("mif:application/vnd.mif");
        add("mov:video/quicktime");
        add("movie:video/x-sgi-movie");
        add("mp2:audio/mpeg");
        add("mp3:audio/mpeg");
        add("mp4:video/mp4");
        add("mpe:video/mpeg");
        add("mpeg:video/mpeg");
        add("mpg:video/mpeg");
        add("mpga:audio/mpeg");
        add("ms:application/x-troff-ms");
        add("msh:model/mesh");
        add("mxu:video/vnd.mpegurl");
        add("nc:application/x-netcdf");
        add("oda:application/oda");
        add("ogg:application/ogg");
        add("ogv:video/ogv");
        add("pbm:image/x-portable-bitmap");
        add("pct:image/pict");
        add("pdb:chemical/x-pdb");
        add("pdf:application/pdf");
        add("pgm:image/x-portable-graymap");
        add("pgn:application/x-chess-pgn");
        add("pic:image/pict");
        add("pict:image/pict");
        add("png:image/png");
        add("pnm:image/x-portable-anymap");
        add("pnt:image/x-macpaint");
        add("pntg:image/x-macpaint");
        add("ppm:image/x-portable-pixmap");
        add("ppt:application/vnd.ms-powerpoint");
        add("ps:application/postscript");
        add("qt:video/quicktime");
        add("qti:image/x-quicktime");
        add("qtif:image/x-quicktime");
        add("ra:audio/x-pn-realaudio");
        add("ram:audio/x-pn-realaudio");
        add("ras:image/x-cmu-raster");
        add("rdf:application/rdf+xml");
        add("rgb:image/x-rgb");
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
        add("snd:audio/basic");
        add("so:application/octet-stream");
        add("spl:application/x-futuresplash");
        add("src:application/x-wais-source");
        add("sv4cpio:application/x-sv4cpio");
        add("sv4crc:application/x-sv4crc");
        add("svg:image/svg+xml");
        add("swf:application/x-shockwave-flash");
        add("t:application/x-troff");
        add("tar:application/x-tar");
        add("tcl:application/x-tcl");
        add("tex:application/x-tex");
        add("texi:application/x-texinfo");
        add("texinfo:application/x-texinfo");
        add("tif:image/tiff");
        add("tiff:image/tiff");
        add("tr:application/x-troff");
        add("tsv:text/tab-separated-values");
        add("txt:text/plain");
        add("ustar:application/x-ustar");
        add("vcd:application/x-cdlink");
        add("vrml:model/vrml");
        add("vxml:application/voicexml+xml");
        add("wav:audio/x-wav");
        add("wbmp:image/vnd.wap.wbmp");
        add("wbxml:application/vnd.wap.wbxml");
        add("webm:video/webm");
        add("wml:text/vnd.wap.wml");
        add("wmlc:application/vnd.wap.wmlc");
        add("wmls:text/vnd.wap.wmlscript");
        add("wmlsc:application/vnd.wap.wmlscriptc");
        add("wmv:video/x-ms-wmv");
        add("wrl:model/vrml");
        add("xbm:image/x-xbitmap");
        add("xht:application/xhtml+xml");
        add("xhtml:application/xhtml+xml");
        add("xls:application/vnd.ms-excel");
        add("xml:application/xml");
        add("xpm:image/x-xpixmap");
        add("xsl:application/xml");
        add("xslt:application/xslt+xml");
        add("xul:application/vnd.mozilla.xul+xml");
        add("xwd:image/x-xwindowdump");
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
        String keyMimePair = "";
        List<String> writeList;
        List<Map<String, String>> filteredList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            key = line.get("key");
            if (key != null && key.contains("."))
                keyMimePair = key.substring(key.lastIndexOf(".") + 1) + ":" + line.get("mimeType");
            if (!extMimeList.contains(keyMimePair)) filteredList.add(line);
        }
        writeList = typeConverter.convertToVList(filteredList);
        if (writeList.size() > 0) fileMap.writeSuccess(String.join("\n", writeList));
        if (typeConverter.getErrorList().size() > 0)
            fileMap.writeError(String.join("\n", typeConverter.getErrorList()));
    }

    public void closeResource() {
        fileMap.closeWriters();
    }
}
