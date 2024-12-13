from elasticsearch import Elasticsearch
import json
from datetime import datetime

#Method to store data in elasticsearch
def send_data_to_es(data):
    es = Elasticsearch(['http://localhost:9200'])
    # Check if Elasticsearch is up and running
    if es.ping():
        print("Successfully connected to Elasticsearch!")
    else:
        print("Connection to Elasticsearch failed.")

    # Define the index name
    index_name = "my_index"

    response = es.index(index=index_name, document=data)
    # Print the response
    print("Document indexed:", response)

    # Optionally, check if the document was indexed
    doc_id = response['_id']
    print(f"Document ID: {doc_id}")


# Method to get data from elasticsearch
def get_data_from_es():

    # Define the index name
    index_name = "my_index"

    es = Elasticsearch(['http://localhost:9200'])
    response = es.search(index=index_name, body={"query": {"match": {'author': 'John Doe'}}})

    # Optionally, check if the document was indexed
    doc_id = response['_id']
    print(f"Document ID: {doc_id}")

    # Retrieve the document by ID to verify
    retrieved_doc = es.get(index=index_name, id=doc_id)

    print("Retrieve the document by author to verify")
    print(response)
    print(type(response))
    print(response["hits"]["hits"][0]["_source"])


# Main function from where the execution starts
if __name__== "__main__":
    # Define a document to index. to be stored in ES
    document = {
        "timestamp": datetime.now(),
        "title": "Example Document",
        "content": "This is an example document to be indexed in Elasticsearch.",
        "author": "John Doe",
    }

    # Call method to store data in ES
    send_data_to_es(document)
    # Call method to get data from ES
    get_data_from_es()
