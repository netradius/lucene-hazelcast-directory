package com.netradius.lucene.hazelcast;

import com.netradius.lucene.hazelcast.directory.HazelcastDirectory;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HazelcastDirectory tests.
 *
 * @author Dilip S Sisodia
 */
@Slf4j
public class HazelcastLuceneSearchTest {

  private Directory directory;
  private IndexWriter writer;
  private IndexSearcher searcher;
  private IndexReader reader;
  private QueryParser parser;
  private List<String> docs = new ArrayList<>();

  @Before
  public void setup() throws IOException {
    docs.add("A Directory is a flat list of files.");
    docs.add("Files may be written once, when they are created.");
    docs.add("Once a file is created it may only be opened for read,or deleted.");
    docs.add("Random access is permitted both when reading and writing.");
    docs.add("Java's i/o APIs not used directly, all i/o is through this API.");
    docs.add("This permits things such as:");
    docs.add("implementation of RAM-based indices;");
    docs.add("implementation indices stored in a database, via JDBC;");
    docs.add("implementation of an index as a single file;");
    docs.add("Directory locking is implemented by an instance of LockFactory,");
    docs.add("and can be changed for each Directory instance");
    StandardAnalyzer analyzer = new StandardAnalyzer();
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    directory = new HazelcastDirectory();
    writer = new IndexWriter(directory, config);
    for (String text : docs) {
      writer.addDocument(addDoc(text));
    }
    writer.close();
    parser = new QueryParser("title", analyzer);
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  @Test
  public void searchTest() {
    try {
      Query q = parser.parse("directory");
      log.debug("Number of documents: " + reader.getDocCount("title"));
      ScoreDoc[] scoreDocs = searcher.search(q, 100).scoreDocs;
      for (ScoreDoc doc : scoreDocs) {
        Document document = searcher.doc(doc.doc);
        log.debug(document.get("title"));
        Assert.assertEquals(true, docs.contains(document.get("title")));
      }
      directory.close();
    } catch (ParseException ex) {
      log.error("Parse exception: " + ex.getMessage());
    } catch (IOException ex) {
      log.error("IOException: " + ex.getMessage());
    } catch (Exception ex) {
      log.error("An exception occurred: " + ex.getMessage());
    }
  }

  @Test
  public void documentCountTest() {
    try {
      int numberOfDocs = reader.getDocCount("title");
      log.debug("Number of documents: " + numberOfDocs);
      Assert.assertEquals(docs.size(), numberOfDocs);
      directory.close();
    } catch (IOException ex) {
      log.error("IOException: " + ex.getMessage());
    } catch (Exception ex) {
      log.error("An exception occurred: " + ex.getMessage());
    }
  }

  private Document addDoc(String value) throws IOException {
    Document doc = new Document();
    doc.add(new TextField("title", value, Field.Store.YES));
    return doc;
  }
}
