import json
import os
import time
import logging
import argparse
from ibm_watson import DiscoveryV2
from ibm_watson.discovery_v2 import QueryLargePassages
from ibm_cloud_sdk_core import get_authenticator_from_environment
from ibm_cloud_sdk_core.authenticators import Authenticator

WD_CURSOR_KEY = 'wd_cursor_key'

parser = argparse.ArgumentParser()
parser.add_argument("service_url", help="Watson Discovery Service URL")
parser.add_argument("project_id", help="ID of Watson Discovery Project")
parser.add_argument("collection_id", help="ID of Watson Discovery Collection of specified project to insert metadata to scroll all documents")
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
  metadata,
  cursor_value
):
  metadata[WD_CURSOR_KEY] = cursor_value
  discovery.update_document(
    project_id=project_id,
    collection_id=collection_id,
    document_id=document_id,
    metadata=json.dumps(metadata)
  ).get_result()

def update_docs(service_url, project_id, collection_id):
  authenticator = get_authenticator_from_environment("WATSON")

  discovery = DiscoveryV2(version="2022-12-31", authenticator=authenticator)
  discovery.set_service_url(service_url)
  discovery.set_disable_ssl_verification(True)

  counter = serial_number_counter()

  while True:
    query_result = discovery.query(
      project_id=project_id,
      collection_ids=[collection_id],
      filter=f"metadata.{WD_CURSOR_KEY}:!*",
      return_=['document_id', 'metadata'],
      aggregation='',
      passages=QueryLargePassages(enabled=False)
    ).get_result()

    if query_result['matching_results'] == 0:
      logger.info(f"all documents has cursor key")
      break
    else:
      logger.info(f"remaining {query_result['matching_results']}")
      for result in query_result['results']:
        document_detail = discovery.get_document(project_id, collection_id, result['document_id']).get_result()
        if document_detail['status'] == 'available':
          # we can update document that is already indexed
          update_document(
            discovery,
            project_id, collection_id, result['document_id'],
            result['metadata'], counter.__next__()
          )
          logger.info(f"updated {result['document_id']}")
        else:
          logger.info(f"waiting {result['document_id']}")
      time.sleep(10)

if __name__ == '__main__':
  update_docs(args.service_url, args.project_id, args.collection_id)