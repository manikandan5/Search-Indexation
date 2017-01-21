
import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;

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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class GenerateIndex {

	private void index(String indexDirectory1, String dataDirectory1, String fileType) throws Exception 
	{
		File dataDirectory = new File(dataDirectory1);
		
		Directory indexDirectory = FSDirectory.open(Paths.get(indexDirectory1));
		
		StandardAnalyzer indexAnalyzer = new StandardAnalyzer();

		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(indexAnalyzer);

		indexWriterConfig.setOpenMode(OpenMode.CREATE);

		IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);

		ArrayList<String> fields = new ArrayList<String>();
		fields.add("DOCNO");
		fields.add("FILEID");
		fields.add("FIRST");
		fields.add("SECOND");
		fields.add("TEXT");
		fields.add("HEAD");
		fields.add("BYLINE");
		fields.add("DATELINE");
		
		File[] files = dataDirectory.listFiles();
		
		Boolean indexing = false;
		for(int i=0; i < files.length; i++)
		{
			if(files[i].getName().endsWith(fileType))
			{
				System.out.println("Indexing file " + files[i].getAbsolutePath());
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
		if(indexing)
		{
			System.out.println("Indexing completed successfully");
		}
			
		indexWriter.forceMerge(1);
		indexWriter.commit();
		indexWriter.close();

		
		IndexReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDirectory1)));
		
		//Print the vocabulary for <field>TEXT</field>
		Terms vocabulary = MultiFields.getTerms(indexReader, "TEXT");
		
		TermsEnum iterator = vocabulary.iterator();
		BytesRef byteRef = null;
		System.out.println("\n*******Vocabulary-Start**********");
		while((byteRef = iterator.next()) != null) 
		{
			String term = byteRef.utf8ToString();
			System.out.print(term+"\t");
		}
		System.out.println("\n*******Vocabulary-End**********");
		
		//Print the total number of documents in the corpus
		System.out.println("Total number of documents in the corpus: "+indexReader.maxDoc());
		
		//Print the number of documents containing the term "new" in <field>TEXT</field>.
		System.out.println("Number of documents containing the term \"new\" for field \"TEXT\": "+indexReader.docFreq(new Term("TEXT", "new")));
		
		//Print the total number of occurrences of the term "new" across all documents for <field>TEXT</field>.
		System.out.println("Number of occurrences of \"new\" in the field \"TEXT\": "+indexReader.totalTermFreq(new Term("TEXT","new")));
		
		//Print the size of the vocabulary for <field>TEXT</field>, applicable when the index has only one segment.
		System.out.println("Size of the vocabulary for this field: "+vocabulary.size());
		
		//Print the total number of documents that have at least one term for <field>TEXT</field>
		System.out.println("Number of documents that have at least one term for this field: "+vocabulary.getDocCount());
		
		//Print the total number of tokens for <field>TEXT</field>
		System.out.println("Number of tokens for this field: "+vocabulary.getSumTotalTermFreq());
		
		//Print the total number of postings for <field>TEXT</field>
		System.out.println("Number of postings for this field: "+vocabulary.getSumDocFreq());
		
		indexReader.close();

	}

	//Main
	public static void main(String[] args) throws Exception 
	{
		String currPath = System.getProperty("user.dir");
		
		//Folder in which the index would be created
		String indexFolder = currPath + "/Index/";
		
		//Folder path for the data corpus
		String dataFolder = currPath + "/corpus/";
		
		//Input file type
		String inputType = "trectext";
		
		//Instance of the class
		GenerateIndex corpusIndex = new GenerateIndex();
		
		//Function call to generate index
		corpusIndex.index(indexFolder, dataFolder, inputType);
	}
}
