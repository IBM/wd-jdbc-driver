import io
import json
import os
import time
import logging
import argparse
from ibm_watson import DiscoveryV2
from ibm_watson.discovery_v2 import QueryLargePassages, QueryLargeTableResults
from ibm_cloud_sdk_core import get_authenticator_from_environment
from ibm_cloud_sdk_core.authenticators import Authenticator

WD_CURSOR_KEY = 'wd_cursor_key'

parser = argparse.ArgumentParser()
parser.add_argument("service_url", help="Watson Discovery Service URL")
parser.add_argument("source_project_id", help="ID of Watson Discovery Project")
parser.add_argument("source_collection_id", help="ID of Watson Discovery Collection of specified project to insert metadata to scroll all documents")
parser.add_argument("target_project_id", help="ID of Watson Discovery Project")
parser.add_argument("target_collection_id", help="ID of Watson Discovery Collection of specified project to insert metadata to scroll all documents")
args = parser.parse_args()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

def serial_number_counter(start=0):
  while True:
    yield start
    start += 1

def update_document(
  discovery,
  project_id,
  collection_id,
  document_id,
  body,
  metadata,
  filename,
  content_type,
  cursor_value
):
  metadata[WD_CURSOR_KEY] = cursor_value
  discovery.update_document(
    project_id=project_id,
    collection_id=collection_id,
    document_id=document_id,
    file=body,
    metadata=json.dumps(metadata),
    filename=filename,
    file_content_type=content_type
  ).get_result()

def update_docs(service_url, source_project_id, source_collection_id, target_project_id, target_collection_id):
  authenticator = get_authenticator_from_environment("WATSON")

  discovery = DiscoveryV2(version="2022-12-31", authenticator=authenticator)
  discovery.set_service_url(service_url)
  discovery.set_disable_ssl_verification(True)

  counter = serial_number_counter()

  query_result = discovery.query(
    project_id=source_project_id,
    collection_ids=[source_collection_id],
    aggregation='',
    passages=QueryLargePassages(enabled=False),
    table_results=QueryLargeTableResults(enabled=False),
    count=100,
  ).get_result()

  for result in query_result['results']:
    document_id = result['document_id']
    new_source = json.loads(json.dumps(result))
    metadata = new_source['metadata']
    filename = new_source['extracted_metadata']['filename']
    content_type = 'application/json'
    del metadata['parent_document_id']
    fields_to_delete = []
    for key in list(new_source.keys()):
      if key == 'document_id':
        fields_to_delete.append(key)
      elif key == 'extracted_metadata':
        fields_to_delete.append(key)
      elif key == 'metadata':
        fields_to_delete.append(key)
      elif key == 'result_metadata':
        fields_to_delete.append(key)
      elif key.startswith('enriched_'):
        new_source[f"copied_{key}"] = new_source[key]
        fields_to_delete.append(key)
    for field in fields_to_delete:
      del new_source[field]
    update_document(
      discovery,
      target_project_id, target_collection_id, document_id,
      io.StringIO(json.dumps(new_source)), metadata, filename, content_type, counter.__next__()
    )
    logger.info(f"updated {result['document_id']}")

if __name__ == '__main__':
  update_docs(args.service_url, args.source_project_id, args.source_collection_id, args.target_project_id, args.target_collection_id)