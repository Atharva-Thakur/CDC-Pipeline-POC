from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchFieldDataType,
    SearchableField,
)
import config

def create_search_index():
    if not config.AZURE_SEARCH_ENDPOINT or not config.AZURE_SEARCH_API_KEY:
        print("Azure Search credentials not found in env.")
        return

    endpoint = config.AZURE_SEARCH_ENDPOINT
    key = config.AZURE_SEARCH_API_KEY
    index_name = config.AZURE_SEARCH_INDEX_NAME

    credential = AzureKeyCredential(key)
    client = SearchIndexClient(endpoint=endpoint, credential=credential)

    # Define the index schema
    # We will transform first_name + last_name -> full_name
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SearchableField(name="full_name", type=SearchFieldDataType.String, sortable=True),
        SearchableField(name="email", type=SearchFieldDataType.String),
        SearchableField(name="street", type=SearchFieldDataType.String),
        SearchableField(name="city", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SearchableField(name="zip_code", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="last_updated_by", type=SearchFieldDataType.String),
        SimpleField(name="processed_at", type=SearchFieldDataType.String) # Changed to String to match UTC ISO string sent by pipeline
    ]

    index = SearchIndex(name=index_name, fields=fields)

    print(f"Creating or updating index '{index_name}'...")
    try:
        result = client.create_or_update_index(index)
        print(f"Index created/updated: {result.name}")
    except Exception as e:
        print(f"Error creating index: {e}")

if __name__ == "__main__":
    create_search_index()
