A simple java program to index pdf/doc/docx/rtf/htm/html/odt articles using Elasticsearch.

Install ingest attachment plugin and type this command to Kibana console before all:
```
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "indexed_chars" : "-1"
      }
    }
  ]
}
```
This is the way how pdf_index cleans up all little html trash files:
```
POST crystal/_delete_by_query
{
  "query": {
    "range" : {
      "attachment.content_length" : {
        "lte" : 1000
      }
    }
  }
}
```
 Crystal index configured with synonym filter,
 with a path of analysis/synonym.txt (relative to the elasticsearch config location),
 so copy synonym.txt file there, or just use your own file.

 Next command is implemented:
 ```
PUT crystal
{
  "settings": {
    "index" : {
      "analysis" : {
        "analyzer" : {
          "crystal" : {
            "tokenizer" : "standard",
            "filter" : ["standard", "lowercase", "english_stopwords", "russian_stopwords", "english_stemmer",  "russian_stemmer", "crystal_synonyms"]
          }
        },
        "filter" : {
          "crystal_synonyms" : {
            "type" : "synonym",
            "synonyms_path" : "analysis/synonym.txt"
          },
          "english_stopwords": {
            "type": "stop",
            "stopwords": "_english_"
          },
          "russian_stopwords": {
            "type": "stop",
            "stopwords": "_russian_"
          },
          "russian_stemmer" : {
            "type" : "stemmer",
            "name" : "russian"
          },
          "english_stemmer" : {
            "type" : "stemmer",
            "name" : "english"
          }
        }
      }
    }
  },
  "mappings": {
    "article": {
      "properties": {
        "attachment.content": {
          "type": "text",
          "analyzer": "crystal"
        }
      }
    }
  }
}
```
