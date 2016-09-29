import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class IndexComparison 
{
	
	private void analyzerFunc(File indexDirectory, File dataCorpus, String fileType, String AnalyzerType) throws Exception
	{
		Directory indexDir = FSDirectory.open(Paths.get(indexDirectory.getAbsolutePath()));
		IndexWriterConfig iwc;
		
		if(AnalyzerType == "keyWord")
		{
			KeywordAnalyzer analyzer = new KeywordAnalyzer();
			iwc = new IndexWriterConfig(analyzer);
			
		}
		else if(AnalyzerType == "simple")
		{
			SimpleAnalyzer analyzer = new SimpleAnalyzer();
			iwc = new IndexWriterConfig(analyzer);
		}
		else if(AnalyzerType == "stop")
		{
			StopAnalyzer analyzer = new StopAnalyzer();
			iwc = new IndexWriterConfig(analyzer);
		}
		else
		{
			StandardAnalyzer analyzer = new StandardAnalyzer();
			iwc = new IndexWriterConfig(analyzer);
		}

		iwc.setOpenMode(OpenMode.CREATE);
		
		IndexWriter indexWriter = new IndexWriter(indexDir, iwc);
		
		File[] files = dataCorpus.listFiles();
		
		Boolean indexing = false;
		
		ArrayList<String> fields = new ArrayList<String>();
		
		fields.add("DOCNO");
		fields.add("FILEID");
		fields.add("FIRST");
		fields.add("SECOND");
		fields.add("TEXT");
		fields.add("HEAD");
		fields.add("BYLINE");
		fields.add("DATELINE");
		
		System.out.println("Indexing file - "+ AnalyzerType);
		for(int i=0; i < files.length; i++)
		{
			if(files[i].getName().endsWith(fileType))
			{
				//System.out.println("Indexing file " + files[i].getAbsolutePath());
				System.out.print(".");
				String fileContent = new String(Files.readAllBytes(Paths.get(files[i].getAbsolutePath())));
				String[] docs = fileContent.split("</DOC>");
				
				for(String docContent : docs)
				{
					Document document = new Document();
					for(int k = 0; k < fields.size(); k++)
					{
						String fieldContent = "";
						
						StringBuffer strBuffer = new StringBuffer();
						
						int start = 0;
						
						while ((start = docContent.indexOf("<" + fields.get(k) + ">", start)) != -1)
						{
							start += fields.get(k).length() + 2;
							int end = docContent.indexOf("</" + fields.get(k) + ">", start);
							String fieldValue = docContent.substring(start, end);
							strBuffer.append(fieldValue);
							start += fieldValue.length();
						}
						fieldContent = strBuffer.toString();
						
						if(k == 0)
						{
							document.add(new StringField("DOCNO", fieldContent, Field.Store.YES));
						}
						else
						{
							document.add(new TextField(fields.get(k), fieldContent, Field.Store.YES));
						}
						
					}
					indexWriter.addDocument(document);
				}
					
				indexing = true;
			}
			else
			{
				indexing = false;
			}
				
		}
		System.out.println();
		if(indexing)
		{
			System.out.println(AnalyzerType + " indexing completed successfully");
		}
			
		indexWriter.forceMerge(1);
		indexWriter.commit();
		indexWriter.close();
		
		IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDirectory.getAbsolutePath())));
		
		//Print the vocabulary for <field>TEXT</field>
		Terms vocabulary = MultiFields.getTerms(indexReader, "TEXT");
		
		/*
		TermsEnum iterator = vocabulary.iterator();
		BytesRef byteRef = null;
		
		System.out.println("Vocabulary for TEXT field using " + AnalyzerType);
		
		System.out.println("\n*******Vocabulary-Start**********");
		while((byteRef = iterator.next()) != null) 
		{
			String term = byteRef.utf8ToString();
			System.out.print(term+"\t");
		}
		System.out.println("\n*******Vocabulary-End**********");*/
		
		//Print the total number of documents in the corpus
		System.out.println(AnalyzerType + "- Total number of documents in the corpus: "+indexReader.maxDoc());
		
		//Print the number of documents containing the term "new" in <field>TEXT</field>.
		System.out.println(AnalyzerType + "- Number of documents containing the term \"new\" for field \"TEXT\": "+indexReader.docFreq(new Term("TEXT", "new")));
		
		//Print the total number of occurrences of the term "new" across all documents for <field>TEXT</field>.
		System.out.println(AnalyzerType + "- Number of occurrences of \"new\" in the field \"TEXT\": "+indexReader.totalTermFreq(new Term("TEXT","new")));
		
		//Print the size of the vocabulary for <field>TEXT</field>, applicable when the index has only one segment.
		System.out.println(AnalyzerType + "- Size of the vocabulary for this field: "+vocabulary.size());
		
		//Print the total number of documents that have at least one term for <field>TEXT</field>
		System.out.println(AnalyzerType + "- Number of documents that have at least one term for this field: "+vocabulary.getDocCount());
		
		//Print the total number of tokens for <field>TEXT</field>
		System.out.println(AnalyzerType + "- Number of tokens for this field: "+vocabulary.getSumTotalTermFreq());
		
		//Print the total number of postings for <field>TEXT</field>
		System.out.println(AnalyzerType + "- Number of postings for this field: "+vocabulary.getSumDocFreq());
		
		indexReader.close();
		
		System.out.println("\n\n");
	}
		
	//Main
	public static void main(String[] args) throws Exception 
	{
		String currPath = System.getProperty("user.dir");
		
		File keyWordIndex = new File(currPath + "/Indices/keyWordIndex");
		
		File simpleIndex = new File(currPath + "/Indices/simpleIndex");
		
		File stopIndex = new File(currPath + "/Indices/stopIndex");
		
		File standardIndex = new File(currPath + "/Indices/standardIndex");
		
		File dataCorpus = new File(currPath + "/corpus/");
		
		String fileType = "trectext";
		
		IndexComparison comparitiveStudy = new IndexComparison();
		
		comparitiveStudy.analyzerFunc(keyWordIndex, dataCorpus, fileType, "keyWord");
		comparitiveStudy.analyzerFunc(simpleIndex, dataCorpus, fileType, "simple");
		comparitiveStudy.analyzerFunc(stopIndex, dataCorpus, fileType, "stop");
		comparitiveStudy.analyzerFunc(standardIndex, dataCorpus, fileType, "standard");		
	}
}