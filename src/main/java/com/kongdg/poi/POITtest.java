package com.kongdg.poi;

import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.model.PAPX;
import org.apache.poi.hwpf.model.PicturesTable;
import org.apache.poi.hwpf.usermodel.Picture;
import org.apache.poi.ooxml.extractor.POIXMLTextExtractor;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFPictureData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author userkdg
 * @date 2020-06-15 21:13
 **/
public class POITtest {
    public static void main(String[] args) {
        // String path = "D:\\temp\\temp\\test.doc";
        String path = "F:\\\\WeChat Files\\\\wxid_abv0kt174dwl21\\\\FileStorage\\\\File\\\\2020-06\\\\学易金卷：段考模拟君之2019-2020学年九年级数学上学期期末原创卷B卷（广东）（考试版）【测试范围：人教版九上全册、九下全册】.doc";
        String content = null;
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            InputStream is = null;
            HWPFDocument doc = null;
            XWPFDocument docx = null;
            POIXMLTextExtractor extractor = null;
            try {
                is = new FileInputStream(file);
                if (path.endsWith(".doc")) {
                    doc = new HWPFDocument(is);

                    // 文档文本内容
                    content = doc.getDocumentText();
                    StringBuilder text = doc.getText();
                    ArrayList<PAPX> paragraphs = doc.getParagraphTable().getParagraphs();
                    for (PAPX paragraph : paragraphs) {
                        System.out.println(text.substring(paragraph.getStart(), paragraph.getEnd()));
                    }
                    // 文档图片内容
                    PicturesTable picturesTable = doc.getPicturesTable();
                    List<Picture> pictures = picturesTable.getAllPictures();
                    for (Picture picture : pictures) {
                        // 输出图片到磁盘
                        OutputStream out = new FileOutputStream(
                                new File("D:\\temp\\" + SnowFlake.getInstance().nextId() + "." + picture.suggestFileExtension()));
                        picture.writeImageContent(out);
                        out.close();
                    }
                    doc.write(new File("D:\\temp\\test.txt"));
                } else if (path.endsWith("docx")) {
                    docx = new XWPFDocument(is);
                    extractor = new XWPFWordExtractor(docx);

                    // 文档文本内容
                    content = extractor.getText();

                    // 文档图片内容
                    List<XWPFPictureData> pictures = docx.getAllPictures();
                    for (XWPFPictureData picture : pictures) {
                        byte[] bytev = picture.getData();
                        // 输出图片到磁盘
                        FileOutputStream out = new FileOutputStream(
                                "D:\\temp\\temp\\" + UUID.randomUUID() + picture.getFileName());
                        out.write(bytev);
                        out.close();
                    }
                } else {
                    System.out.println("此文件不是word文件！");
                }
                System.out.println(content);
            } catch (FileNotFoundException e) {
            } catch (IOException e) {
            } finally {
                try {
                    if (doc != null) {
                        doc.close();
                    }
                    if (extractor != null) {
                        extractor.close();
                    }
                    if (docx != null) {
                        docx.close();
                    }
                    if (is != null) {
                        is.close();
                    }
                } catch (IOException e) {
                }
            }
        }
    }
}
